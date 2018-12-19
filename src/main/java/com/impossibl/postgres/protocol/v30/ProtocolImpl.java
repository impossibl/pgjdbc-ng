/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.Command;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FunctionCallCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.PrepareExecCommand;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.SSLRequestCommand;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.StartupCommand;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.Context.KeyData;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Failed;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static com.impossibl.postgres.utils.ByteBufs.lengthEncode;
import static com.impossibl.postgres.utils.ByteBufs.readCString;
import static com.impossibl.postgres.utils.ByteBufs.writeCString;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINEST;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.AttributeKey;

public class ProtocolImpl implements Protocol {

  private static final Logger LOGGER = Logger.getLogger("com.impossibl.postgres.protocol.v30");

  private static final AttributeKey<ProtocolImpl> PROTOCOL_KEY = AttributeKey.valueOf("protocol");

  public abstract static class ExecutionTimerTask implements Callable<Void> {

    enum State {
      NotStarted,
      Running,
      Cancelling,
      Completed
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.NotStarted);
    private Thread thread;

    public abstract void run();

    @Override
    public Void call() {

      try {

        thread = Thread.currentThread();

        if (!state.compareAndSet(State.NotStarted, State.Running))
          return null;

        run();

      }
      catch (Throwable e) {
        // Ignore...
      }
      finally {
        state.set(State.Completed);
        synchronized (state) {
          state.notify();
        }
      }
      return null;
    }

    void cancel() {

      if (this.state.getAndSet(State.Cancelling) == State.Running) {

        thread.interrupt();

        synchronized (state) {

          while (state.get() == State.Cancelling) {
            try {
              state.wait();
            }
            catch (InterruptedException e) {
              // Ignore
            }
          }
        }

      }

    }

  }

  // Frontend messages
  private static final byte PASSWORD_MSG_ID = 'p';
  private static final byte FLUSH_MSG_ID = 'H';
  private static final byte TERMINATE_MSG_ID = 'X';
  private static final byte SYNC_MSG_ID = 'S';
  private static final byte QUERY_MSG_ID = 'Q';
  private static final byte PARSE_MSG_ID = 'P';
  private static final byte BIND_MSG_ID = 'B';
  private static final byte DESCRIBE_MSG_ID = 'D';
  private static final byte EXECUTE_MSG_ID = 'E';
  private static final byte CLOSE_MSG_ID = 'C';
  private static final byte FUNCTION_CALL_MSG_ID = 'F';

  // Backend messages
  private static final byte BACKEND_KEY_MSG_ID = 'K';
  private static final byte AUTHENTICATION_MSG_ID = 'R';
  private static final byte ERROR_MSG_ID = 'E';
  private static final byte NOTICE_MSG_ID = 'N';
  private static final byte NOTIFICATION_MSG_ID = 'A';
  private static final byte COMMAND_COMPLETE_MSG_ID = 'C';
  private static final byte PARAMETER_STATUS_MSG_ID = 'S';
  private static final byte READY_FOR_QUERY_MSG_ID = 'Z';
  private static final byte PARAMETER_DESC_MSG_ID = 't';
  private static final byte ROW_DESC_MSG_ID = 'T';
  private static final byte ROW_DATA_MSG_ID = 'D';
  private static final byte PORTAL_SUSPENDED_MSG_ID = 's';
  private static final byte NO_DATA_MSG_ID = 'n';
  private static final byte EMPTY_QUERY_MSG_ID = 'I';
  private static final byte PARSE_COMPLETE_MSG_ID = '1';
  private static final byte BIND_COMPLETE_MSG_ID = '2';
  private static final byte CLOSE_COMPLETE_MSG_ID = '3';
  private static final byte FUNCTION_RESULT_MSG_ID = 'V';

  private final ProtocolListener nullListener = new BaseProtocolListener() {

    @Override
    public void exception(Throwable cause) {
    }

  };

  private final InetSocketAddress remote;
  private AtomicBoolean connected = new AtomicBoolean(true);
  private ProtocolShared.Ref sharedRef;
  private Channel channel;
  private WeakReference<BasicContext> contextRef;
  private TransactionStatus txStatus;
  private ProtocolListener listener;
  private ScheduledFuture<?> executionTimeout;
  private ExecutionTimerTask task;

  private ProtocolImpl(ProtocolShared.Ref sharedRef, Channel channel, BasicContext context) {
    this.sharedRef = sharedRef;
    this.channel = channel;
    this.contextRef = new WeakReference<>(context);
    this.txStatus = Idle;
    this.remote = (InetSocketAddress) channel.remoteAddress();
  }

  public BasicContext getContext() {
    return contextRef.get();
  }

  public Channel getChannel() {
    return channel;
  }

  @Override
  public boolean isConnected() {
    return connected.get() && channel.isActive();
  }

  @Override
  public void shutdown() {

    //Ensure only one thread can ever succeed in calling shutdown
    if (!connected.getAndSet(false)) {
      return;
    }

    try {
      ByteBuf msg = channel.alloc().buffer();
      writeTerminate(msg);
      channel.writeAndFlush(msg).addListener(ChannelFutureListener.CLOSE);
    }
    catch (Exception e) {
      //Close anyway...
      channel.close().awaitUninterruptibly(100);
    }

    sharedRef.release();
  }

  private void kill() {
    connected.set(false);
    channel.close().awaitUninterruptibly();
  }

  @Override
  public void abort(Executor executor) {

    if (!connected.get())
      return;

    //Shutdown socket (also guarantees no more commands begin execution)
    shutdown();

    //Issue cancel request from separate socket (per Postgres protocol). This
    //is a convenience to the server as the abort does not depend on its
    //success to complete properly

    executor.execute(this::sendCancelRequest);

    this.listener.abort();

  }

  void setListener(ProtocolListener listener) {
    this.listener = listener;
  }

  @Override
  public SSLRequestCommand createSSLRequest() {
    return new SSLRequestCommandImpl();
  }

  @Override
  public StartupCommand createStartup(Map<String, Object> settings) {
    return new StartupCommandImpl(settings);
  }

  @Override
  public PrepareCommand createPrepare(String statementName, String sqlText, Type[] parameterTypes) {
    return new PrepareCommandImpl(statementName, sqlText, parameterTypes);
  }

  @Override
  public BindExecCommand createBindExec(String portalName, String statementName, FieldFormat[] parameterFormats, ByteBuf[] parameterBuffers, ResultField[] resultFields) {
    return new BindExecCommandImpl(portalName, statementName, parameterFormats, parameterBuffers, resultFields);
  }

  @Override
  public PrepareExecCommand createPrepareExec(String sql, String portalName, ResultField[] resultFields) {
    return new PrepareExecCommandImpl(sql, portalName, resultFields);
  }

  @Override
  public QueryCommand createQuery(String sqlText) {
    return new QueryCommandImpl(sqlText);
  }

  @Override
  public FunctionCallCommand createFunctionCall(String functionName, FieldFormat[] parameterFormats, ByteBuf[] parameterBuffers) {
    return new FunctionCallCommandImpl(functionName, parameterFormats, parameterBuffers);
  }

  @Override
  public CloseCommand createClose(ServerObjectType objectType, String objectName) {
    return new CloseCommandImpl(objectType, objectName);
  }

  void enableExecutionTimer(ExecutionTimerTask task, long timeout) {

    if (executionTimeout != null) {
      throw new IllegalStateException("execution timer already enabled");
    }
    this.task = task;
    executionTimeout = channel.eventLoop().schedule(task, timeout, MILLISECONDS);
  }

  private void cancelExecutionTimer() {

    if (executionTimeout != null) {

      try {
        executionTimeout.cancel(true);

        //Ensure any task that is currently running also gets
        //completely cancelled, or finishes, before returning
        task.cancel();

      }
      finally {
        task = null;
        executionTimeout = null;
      }

    }

  }

  @Override
  public synchronized void execute(Command cmd) throws IOException {

    if (!(cmd instanceof CommandImpl))
      throw new IllegalArgumentException();

    if (!connected.get() || !channel.isActive()) {
      throw new InterruptedIOException("channel closed");
    }

    try {

      ((CommandImpl) cmd).execute(this);

      Throwable exception = cmd.getException();
      if (exception != null) {
        if (exception instanceof RuntimeException) {
          throw (RuntimeException) exception;
        }
        else if (exception instanceof IOException) {
          throw (IOException) exception;
        }
        else {
          throw new IOException(exception.getCause());
        }
      }

      if (cmd.getError() != null) {
        if (cmd.getError().getCode().startsWith(Notice.CONNECTION_EXC_CLASS)) {
          kill();
        }
      }

    }
    catch (InterruptedIOException e) {

      sendCancelRequest();

      throw e;
    }
    finally {

      cancelExecutionTimer();

      //Ensure listener is reset
      listener = nullListener;
    }
  }

  @Override
  public TransactionStatus getTransactionStatus() {
    return txStatus;
  }

  void writeSSLRequest(ByteBuf msg) {

    msg.writeInt(8);
    msg.writeInt(80877103);

  }

  void writeStartup(ByteBuf msg, Map<String, Object> params) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("STARTUP: " + params);

    beginMessage(msg, (byte) 0);

    // Version
    msg.writeShort(3);
    msg.writeShort(0);

    // Name=Value pairs
    for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
      writeCString(msg, paramEntry.getKey(), context.getCharset());
      writeCString(msg, paramEntry.getValue().toString(), context.getCharset());
    }

    msg.writeByte(0);

    endMessage(msg);
  }

  void writePassword(ByteBuf msg, String password) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("PASSWORD: " + password);

    beginMessage(msg, PASSWORD_MSG_ID);

    writeCString(msg, password, context.getCharset());

    endMessage(msg);
  }

  void writeQuery(ByteBuf msg, String query) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("QUERY: " + query);

    beginMessage(msg, QUERY_MSG_ID);

    writeCString(msg, query, context.getCharset());

    endMessage(msg);
  }

  void writeParse(ByteBuf msg, String stmtName, String query, Type[] paramTypes) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("PARSE (" + stmtName + "): " + query);

    beginMessage(msg, PARSE_MSG_ID);

    writeCString(msg, stmtName != null ? stmtName : "", context.getCharset());
    writeCString(msg, query, context.getCharset());

    msg.writeShort(paramTypes.length);
    for (Type paramType : paramTypes) {
      int paramTypeOid = paramType != null ? paramType.getId() : 0;
      msg.writeInt(paramTypeOid);
    }

    endMessage(msg);
  }

  void writeBind(ByteBuf msg, String portalName, String stmtName, FieldFormat[] parameterFormats, ByteBuf[] parameterBuffers, FieldFormat[] resultFieldFormats) throws IOException {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("BIND (" + portalName + "): " + parameterBuffers.length);

    byte[] portalNameBytes = nullToEmpty(portalName).getBytes(context.getCharset());
    byte[] stmtNameBytes = nullToEmpty(stmtName).getBytes(context.getCharset());

    beginMessage(msg, BIND_MSG_ID);

    writeCString(msg, portalNameBytes);
    writeCString(msg, stmtNameBytes);

    loadParams(msg, parameterFormats, parameterBuffers);

    //Set format for results fields
    if (resultFieldFormats == null || resultFieldFormats.length == 0) {
      //Request all binary
      msg.writeShort(1);
      msg.writeShort(1);
    }
    else {
      //Select result format for each
      msg.writeShort(resultFieldFormats.length);
      for (FieldFormat format : resultFieldFormats) {
        msg.writeShort(format.ordinal());
      }
    }

    endMessage(msg);
  }

  void writeDescribe(ByteBuf msg, ServerObjectType target, String targetName) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("DESCRIBE " + target + " (" + targetName + ")");

    beginMessage(msg, DESCRIBE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", context.getCharset());

    endMessage(msg);
  }

  void writeExecute(ByteBuf msg, String portalName, int maxRows) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("EXECUTE (" + portalName + "): " + maxRows);

    beginMessage(msg, EXECUTE_MSG_ID);

    writeCString(msg, portalName != null ? portalName : "", context.getCharset());
    msg.writeInt(maxRows);

    endMessage(msg);
  }

  void writeFunctionCall(ByteBuf msg, int functionId, FieldFormat[] parameterFormats, ByteBuf[] parameterBuffers) throws IOException {

    beginMessage(msg, FUNCTION_CALL_MSG_ID);

    msg.writeInt(functionId);

    loadParams(msg, parameterFormats, parameterBuffers);

    msg.writeShort(1);

    endMessage(msg);
  }

  void writeClose(ByteBuf msg, ServerObjectType target, String targetName) {

    Context context = getContext();

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("CLOSE " + target + ": " + targetName);

    beginMessage(msg, CLOSE_MSG_ID);

    msg.writeByte(target.getId());
    writeCString(msg, targetName != null ? targetName : "", context.getCharset());

    endMessage(msg);
  }

  void writeFlush(ByteBuf msg) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("FLUSH");

    writeMessage(msg, FLUSH_MSG_ID);
  }

  void writeSync(ByteBuf msg) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("SYNC");

    writeMessage(msg, SYNC_MSG_ID);
  }

  private void writeTerminate(ByteBuf msg) {

    if (LOGGER.isLoggable(FINEST))
      LOGGER.finest("TERM");

    writeMessage(msg, TERMINATE_MSG_ID);
  }

  public void send(ByteBuf msg) {
    channel.writeAndFlush(msg, channel.voidPromise());
  }

  void sendCancelRequest() {

    Context context = getContext();

    LOGGER.finer("CANCEL");

    KeyData keyData = context.getKeyData();

    LOGGER.finest("OPEN-SOCKET");

    try (Socket abortSocket = new Socket(remote.getAddress(), remote.getPort())) {

      LOGGER.finest("SEND-DATA");

      DataOutputStream os = new DataOutputStream(abortSocket.getOutputStream());

      os.writeInt(16);
      os.writeInt(80877102);
      os.writeInt(keyData.getProcessId());
      os.writeInt(keyData.getSecretKey());

    }
    catch (IOException e) {
      //Ignore...
    }

  }

  private void loadParams(ByteBuf buffer, FieldFormat[] paramFormats, ByteBuf[] paramBuffers) throws IOException {

    // Select format for parameters
    if (paramFormats == null) {
      buffer.writeShort(1);
      buffer.writeShort(1);
    }
    else {
      buffer.writeShort(paramFormats.length);
      for (FieldFormat paramFormat : paramFormats) {
        paramFormat = paramFormat != null ? paramFormat : FieldFormat.Text;
        buffer.writeShort(paramFormat.ordinal());
      }
    }

    // Values for each parameter
    if (paramBuffers == null) {
      buffer.writeShort(0);
    }
    else {
      buffer.writeShort(paramBuffers.length);
      for (ByteBuf paramBuffer : paramBuffers) {
        lengthEncode(buffer, paramBuffer, () -> {
          buffer.writeBytes(paramBuffer);
          paramBuffer.resetReaderIndex();
        });
      }
    }
  }

  private void writeMessage(ByteBuf msg, byte msgId) {

    msg.writeByte(msgId);
    msg.writeInt(4);
  }

  private void beginMessage(ByteBuf msg, byte msgId) {

    if (msgId != 0)
      msg.writeByte(msgId);

    msg.markWriterIndex();

    msg.writeInt(-1);
  }

  private void endMessage(ByteBuf msg) {

    int endPos = msg.writerIndex();

    msg.resetWriterIndex();

    int begPos = msg.writerIndex();

    msg.setInt(begPos, endPos - begPos);

    msg.writerIndex(endPos);
  }

  /*
   *
   * Message dispatching & parsing
   */

  void dispatch(byte id, ByteBuf data) throws IOException {

    switch (id) {
      case AUTHENTICATION_MSG_ID:
        receiveAuthentication(data);
        break;

      case BACKEND_KEY_MSG_ID:
        receiveBackendKeyData(data);
        break;

      case PARAMETER_DESC_MSG_ID:
        receiveParameterDescriptions(data);
        break;

      case ROW_DESC_MSG_ID:
        receiveRowDescription(data);
        break;

      case ROW_DATA_MSG_ID:
        receiveRowData(data);
        break;

      case PORTAL_SUSPENDED_MSG_ID:
        receivePortalSuspended();
        break;

      case NO_DATA_MSG_ID:
        receiveNoData();
        break;

      case PARSE_COMPLETE_MSG_ID:
        receiveParseComplete();
        break;

      case BIND_COMPLETE_MSG_ID:
        receiveBindComplete();
        break;

      case CLOSE_COMPLETE_MSG_ID:
        receiveCloseComplete();
        break;

      case EMPTY_QUERY_MSG_ID:
        receiveEmptyQuery();
        break;

      case FUNCTION_RESULT_MSG_ID:
        receiveFunctionResult(data);
        break;

      case ERROR_MSG_ID:
        receiveError(data);
        break;

      case NOTICE_MSG_ID:
        receiveNotice(data);
        break;

      case NOTIFICATION_MSG_ID:
        receiveNotification(data);
        break;

      case COMMAND_COMPLETE_MSG_ID:
        receiveCommandComplete(data);
        break;

      case PARAMETER_STATUS_MSG_ID:
        receiveParameterStatus(data);
        break;

      case READY_FOR_QUERY_MSG_ID:
        receiveReadyForQuery(data);
        break;

      default:
        LOGGER.fine("unsupported message type: " + (id & 0xff));
    }

  }

  void dispatchException(Throwable cause) throws IOException {

    if (listener != null) {
      listener.exception(cause);
    }
  }

  private void receiveAuthentication(ByteBuf buffer) throws IOException {

    int code = buffer.readInt();
    switch (code) {
      case 0:

        // Ok
        listener.authenticated(this);
        return;

      case 2:

        // KerberosV5
        listener.authenticateKerberos(this);
        break;

      case 3:

        // Cleartext
        listener.authenticateClear(this);
        return;

      case 4:

        // Crypt
        listener.authenticateCrypt(this);
        return;

      case 5:

        // MD5
        byte[] salt = new byte[4];
        buffer.readBytes(salt);
        listener.authenticateMD5(this, salt);

        return;

      case 6:

        // SCM Credential
        listener.authenticateSCM(this);
        break;

      case 7:

        // GSS
        listener.authenticateGSS(this);
        break;

      case 8:

        // GSS Continue
        listener.authenticateGSSCont(this);
        break;

      case 9:

        // SSPI
        listener.authenticateSSPI(this);
        break;

      default:
        throw new UnsupportedOperationException("invalid authentication type");
    }
  }

  private void receiveBackendKeyData(ByteBuf buffer) throws IOException {

    int processId = buffer.readInt();
    int secretKey = buffer.readInt();

    listener.backendKeyData(processId, secretKey);
  }

  private void receiveError(ByteBuf buffer) throws IOException {

    Notice notice = parseNotice(buffer);

    LOGGER.finest("ERROR: " + notice.getCode() + ": " + notice.getMessage());

    listener.error(notice);
  }

  private void receiveNotice(ByteBuf buffer) throws IOException {

    Notice notice = parseNotice(buffer);

    LOGGER.finest(notice.getSeverity() + ": " + notice.getCode() + ": " + notice.getMessage());

    listener.notice(notice);
  }

  private void receiveParameterDescriptions(ByteBuf buffer) throws IOException {

    Context context = getContext();

    int paramCount = buffer.readUnsignedShort();

    TypeRef[] paramTypes = new TypeRef[paramCount];

    for (int c = 0; c < paramCount; ++c) {

      int paramTypeId = buffer.readInt();
      paramTypes[c] = TypeRef.from(paramTypeId, context.getRegistry());
    }

    LOGGER.finest("PARAM-DESC: " + paramCount);

    listener.parametersDescription(paramTypes);
  }

  private void receiveRowDescription(ByteBuf buffer) throws IOException {

    Context context = getContext();

    Registry registry = context.getRegistry();

    int fieldCount = buffer.readUnsignedShort();

    ResultField[] fields = new ResultField[fieldCount];

    for (int c = 0; c < fieldCount; ++c) {

      ResultField field = new ResultField(readCString(buffer, context.getCharset()),
                                          buffer.readInt(),
                                          (short)buffer.readUnsignedShort(),
                                          TypeRef.from(buffer.readInt(), registry),
                                          buffer.readShort(),
                                          buffer.readInt(),
                                          FieldFormat.values()[buffer.readUnsignedShort()]);

      fields[c] = field;
    }

    LOGGER.finest("ROW-DESC: " + fieldCount);

    listener.rowDescription(fields);
  }

  private void receiveRowData(ByteBuf buffer) throws IOException {
    LOGGER.finest("DATA");
    listener.rowData(buffer);
  }

  private void receivePortalSuspended() throws IOException {
    LOGGER.finest("SUSPEND");
    listener.portalSuspended();
  }

  private void receiveNoData() throws IOException {
    LOGGER.finest("NO-DATA");
    listener.noData();
  }

  private void receiveCloseComplete() throws IOException {
    LOGGER.finest("CLOSE-COMP");
    listener.closeComplete();
  }

  private void receiveBindComplete() throws IOException {
    LOGGER.finest("BIND-COMP");
    listener.bindComplete();
  }

  private void receiveParseComplete() throws IOException {
    LOGGER.finest("PARSE-COMP");
    listener.parseComplete();
  }

  private void receiveEmptyQuery() throws IOException {
    LOGGER.finest("EMPTY");
    listener.emptyQuery();
  }

  private void receiveFunctionResult(ByteBuf buffer) throws IOException {

    LOGGER.finest("FUNCTION-RES");

    listener.functionResult(buffer);
  }

  private void receiveCommandComplete(ByteBuf buffer) throws IOException {

    Context context = getContext();

    String commandTag = readCString(buffer, context.getCharset());

    String[] parts = commandTag.split(" ");

    String command = parts[0];
    Long rowsAffected = null;
    Long oid = null;

    switch (command) {

      case "INSERT":

        if (parts.length == 3) {

          oid = Long.parseLong(parts[1]);
          rowsAffected = Long.parseLong(parts[2]);
        }
        else {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }

        break;

      case "SELECT":

        if (parts.length == 2) {

          rowsAffected = null;
        }
        else {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }

        break;

      case "UPDATE":
      case "DELETE":
      case "MOVE":
      case "FETCH":

        if (parts.length == 2) {
          rowsAffected = Long.parseLong(parts[1]);
        }
        else {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }

        break;

      case "COPY":

        if (parts.length != 1) {
          if (parts.length == 2) {
            rowsAffected = Long.parseLong(parts[1]);
          }
          else {
            throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
          }
        }

        break;

      case "CREATE":
      case "DROP":
      case "ALTER":
      case "DECLARE":
      case "CLOSE":

        if (parts.length == 2) {

          command += " " + parts[1];
          rowsAffected = 0L;
        }
        else if (parts.length == 3) {
          command += " " + parts[1] + " " + parts[2];
          rowsAffected = 0L;
        }
        else if (parts.length == 4) {
          command += " " + parts[1] + " " + parts[2] + " " + parts[3];
          rowsAffected = 0L;
        }
        else {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }

        break;

      case "PREPARE":
        if (parts.length != 2) {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }
        break;

      case "COMMIT":
        if (parts.length != 1 && parts.length != 2) {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }
        break;

      case "ROLLBACK":
        if (parts.length != 1 && parts.length != 2) {
          throw new IOException("error parsing command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }
        break;

      case "DEALLOCATE":
      case "TRUNCATE":
      case "LOCK":
      case "GRANT":
      case "REVOKE":
        // These are "complex" (e.g. greater than one word) but known good
        break;

      default:

        if (parts.length > 1) {
          LOGGER.warning("Ignoring unknown complex command tag: " + command + " (" + Arrays.toString(parts) + ")");
        }

        rowsAffected = 0L;
    }

    LOGGER.finest("COMPLETE: " + commandTag);

    listener.commandComplete(command, rowsAffected, oid);
  }

  private void receiveNotification(ByteBuf buffer) throws IOException {

    Context context = getContext();

    int processId = buffer.readInt();
    String channelName = readCString(buffer, context.getCharset());
    String payload = readCString(buffer, context.getCharset());

    LOGGER.finest("NOTIFY: " + processId + " - " + channelName + " - " + payload);

    listener.notification(processId, channelName, payload);

    context.reportNotification(processId, channelName, payload);
  }

  private void receiveParameterStatus(ByteBuf buffer) {

    BasicContext context = getContext();

    String name = readCString(buffer, context.getCharset());
    String value = readCString(buffer, context.getCharset());

    context.updateSystemParameter(name, value);
  }

  private void receiveReadyForQuery(ByteBuf buffer) throws IOException {

    switch (buffer.readByte()) {
      case 'T':
        txStatus = Active;
        break;
      case 'E':
        txStatus = Failed;
        break;
      case 'I':
        txStatus = Idle;
        break;
      default:
        throw new IllegalStateException("invalid transaction status");
    }

    LOGGER.finest("READY: " + txStatus);

    listener.ready(txStatus);
  }

  private Notice parseNotice(ByteBuf buffer) {

    Context context = getContext();

    Notice notice = new Notice();

    byte msgId;

    while ((msgId = buffer.readByte()) != 0) {

      switch (msgId) {
        case 'S':
          notice.setSeverity(readCString(buffer, context.getCharset()));
          break;

        case 'C':
          notice.setCode(readCString(buffer, context.getCharset()));
          break;

        case 'M':
          notice.setMessage(readCString(buffer, context.getCharset()));
          break;

        case 'D':
          notice.setDetail(readCString(buffer, context.getCharset()));
          break;

        case 'H':
          notice.setHint(readCString(buffer, context.getCharset()));
          break;

        case 'P':
          notice.setPosition(readCString(buffer, context.getCharset()));
          break;

        case 'W':
          notice.setWhere(readCString(buffer, context.getCharset()));
          break;

        case 'F':
          notice.setFile(readCString(buffer, context.getCharset()));
          break;

        case 'L':
          notice.setLine(readCString(buffer, context.getCharset()));
          break;

        case 'R':
          notice.setRoutine(readCString(buffer, context.getCharset()));
          break;

        case 's':
          notice.setSchema(readCString(buffer, context.getCharset()));
          break;

        case 't':
          notice.setTable(readCString(buffer, context.getCharset()));
          break;

        case 'c':
          notice.setColumn(readCString(buffer, context.getCharset()));
          break;

        case 'd':
          notice.setDatatype(readCString(buffer, context.getCharset()));
          break;

        case 'n':
          notice.setConstraint(readCString(buffer, context.getCharset()));
          break;

        default:
          // Read and ignore
          readCString(buffer, context.getCharset());
          break;
      }

    }

    return notice;
  }

  static ProtocolImpl getAttached(Channel channel) {
    return channel.attr(PROTOCOL_KEY).get();
  }

  static ProtocolImpl newInstance(ProtocolShared.Ref sharedRef, Channel channel, BasicContext context) {
    ProtocolImpl impl = new ProtocolImpl(sharedRef, channel, context);
    channel.attr(PROTOCOL_KEY).set(impl);
    return impl;
  }
}
