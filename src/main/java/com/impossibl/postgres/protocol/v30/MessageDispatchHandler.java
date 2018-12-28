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

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.TypeOid;
import com.impossibl.postgres.protocol.TypeRef;
import com.impossibl.postgres.utils.BlockingReadTimeoutException;

import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Failed;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static com.impossibl.postgres.protocol.v30.ProtocolHandlers.SYNC;
import static com.impossibl.postgres.utils.ByteBufs.readCString;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import static java.util.Arrays.asList;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;


public class MessageDispatchHandler extends ChannelDuplexHandler {

  private TransactionStatus transactionStatus;
  private Deque<ProtocolHandler> protocolHandlers;
  private ProtocolHandler defaultHandler;
  private Charset charset;
  private Writer traceWriter;
  private boolean requiresFlush = false;

  MessageDispatchHandler(Charset charset, Writer traceWriter) {
    this.protocolHandlers = new ConcurrentLinkedDeque<>();
    this.charset = charset;
    this.traceWriter = traceWriter;
  }

  public void setDefaultHandler(ProtocolHandler defaultHandler) {
    this.defaultHandler = defaultHandler;
  }

  TransactionStatus getTransactionStatus() {
    return transactionStatus;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws IOException {

    if (msg instanceof ServerRequest) {

      ServerRequest request = (ServerRequest) msg;

      // Add handler to queue (if request produces one)

      ProtocolHandler requestProtocolHandler = request.createHandler();
      if (requestProtocolHandler != null) {

        protocolHandlers.offer(requestProtocolHandler);

      }

      // Execute the request

      request.execute(new ProtocolChannel(ctx.channel(), ctx, charset));

      promise.setSuccess();
    }
    else if (msg instanceof ByteBuf) {

      // Write msg (after tracing it)

      ByteBuf buf = (ByteBuf) msg;

      trace('<', (char) buf.getByte(0));

      ctx.write(msg,  promise);

      requiresFlush = true;
    }

  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    trace("\n");
    flushTrace();
    if (requiresFlush) {
      super.flush(ctx);
      requiresFlush = false;
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object message) throws IOException {

    ByteBuf msg = (ByteBuf) message;
    try {
      // Parse message header

      byte id = msg.readByte();
      int length = msg.readInt() - 4;
      ByteBuf data = msg.readSlice(length);

      trace('>', (char) id);

      // Dispatch to current request handler

      ProtocolHandler protocolHandler = protocolHandlers.element();

      dispatch(ctx, id, data, protocolHandler);

    }
    finally {
      ReferenceCountUtil.release(msg);
    }
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    trace("\n");
    flushTrace();
    super.channelReadComplete(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {

    // Dispatch to current request handler (if any)

    ProtocolHandler protocolHandler;
    while ((protocolHandler = protocolHandlers.poll()) != null) {
      try {
        protocolHandler.exception(new BlockingReadTimeoutException());
      }
      catch (IOException ignored) {
        // No need to report here...
      }
    }

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

    // Dispatch to current request handler (if any)

    ProtocolHandler protocolHandler = protocolHandlers.poll();
    if (protocolHandler == null) return;

    try {
      protocolHandler.exception(cause);
    }
    catch (IOException ignored) {
    }

  }

  private void dispatch(ChannelHandlerContext ctx, byte id, ByteBuf data, ProtocolHandler protocolHandler) throws IOException {

    ProtocolHandler.Action action;
    try {
      action = parseAndDispatch(ctx, id, data, protocolHandler);
    }
    catch (IOException e) {
      try {
        protocolHandler.exception(e);
      }
      catch (IOException sub) {
        // Failing now will have no real effect, we always sync on pipeline exception
      }
      action = isReadyForQuery(id) ? ProtocolHandler.Action.Complete : ProtocolHandler.Action.Sync;
    }

    if (action == null) {
      if (defaultHandler != null) {
        // Try default handler (which should always return Resume)
        action = parseAndDispatch(ctx, id, data.resetReaderIndex(), defaultHandler);
        if (action != ProtocolHandler.Action.Resume) {
          String failMessage = "Unhandled message: " + (char) id + " @ " + protocolHandler.getClass().getName();
          throw new IllegalStateException(failMessage);
        }
      }
      return;
    }

    switch (action) {
      case Resume:
        trace(".");
        break;

      case ResumePassing:
        trace(".^");
        ProtocolHandler resume = protocolHandlers.pop();
        try {
          dispatch(ctx, id, data.resetReaderIndex(), protocolHandlers.peek());
        }
        finally {
          protocolHandlers.addFirst(resume);
        }
        break;

      case Complete:
        trace("*");
        protocolHandlers.pop();
        break;

      case CompletePassing:
        trace("*^");
        protocolHandlers.pop();
        dispatch(ctx, id, data.resetReaderIndex(), protocolHandlers.peek());
        break;

      case Sync:
        trace("$");
        protocolHandlers.pop();
        protocolHandlers.addFirst(SYNC);
        break;
    }
  }

  // Backend messages
  private static final byte NEGOTIATE_PROTOCOL_VERSION_ID = 'B';
  private static final byte AUTHENTICATION_MSG_ID = 'R';
  private static final byte BACKEND_KEY_MSG_ID = 'K';
  private static final byte PARAMETER_STATUS_MSG_ID = 'S';
  private static final byte ERROR_MSG_ID = 'E';
  private static final byte NOTICE_MSG_ID = 'N';
  private static final byte NOTIFICATION_MSG_ID = 'A';
  private static final byte READY_FOR_QUERY_MSG_ID = 'Z';
  private static final byte COMMAND_COMPLETE_MSG_ID = 'C';
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

  private static boolean isReadyForQuery(byte id) {
    return id == READY_FOR_QUERY_MSG_ID;
  }

  /*
   *
   * Message dispatching & parsing
   */

  private ProtocolHandler.Action parseAndDispatch(ChannelHandlerContext ctx, byte id, ByteBuf data, ProtocolHandler handler) throws IOException {

    switch (id) {
      case NOTIFICATION_MSG_ID:
        if (!(handler instanceof ProtocolHandler.Notification)) return null;
        return receiveNotification(data, (ProtocolHandler.Notification) handler);

      case NOTICE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ReportNotice)) return null;
        return receiveNotice(data, (ProtocolHandler.ReportNotice) handler);

      case NEGOTIATE_PROTOCOL_VERSION_ID:
        if (!(handler instanceof ProtocolHandler.NegotiateProtocolVersion)) return null;
        return receiveNegotiation(data, (ProtocolHandler.NegotiateProtocolVersion) handler);

      case AUTHENTICATION_MSG_ID:
        if (!(handler instanceof ProtocolHandler.Authentication)) return null;
        return receiveAuthentication(ctx, data, (ProtocolHandler.Authentication) handler);

      case BACKEND_KEY_MSG_ID:
        if (!(handler instanceof ProtocolHandler.BackendKeyData)) return null;
        return receiveBackendKeyData(data, (ProtocolHandler.BackendKeyData) handler);

      case PARAMETER_STATUS_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ParameterStatus)) return null;
        return receiveParameterStatus(data, (ProtocolHandler.ParameterStatus) handler);

      case PARAMETER_DESC_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ParameterDescriptions)) return null;
        return receiveParameterDescriptions(data, (ProtocolHandler.ParameterDescriptions) handler);

      case ROW_DESC_MSG_ID:
        if (!(handler instanceof ProtocolHandler.RowDescription)) return null;
        return receiveRowDescription(data, (ProtocolHandler.RowDescription) handler);

      case ROW_DATA_MSG_ID:
        if (!(handler instanceof ProtocolHandler.DataRow)) return null;
        return receiveRowData(data, (ProtocolHandler.DataRow) handler);

      case PORTAL_SUSPENDED_MSG_ID:
        if (!(handler instanceof ProtocolHandler.PortalSuspended)) return null;
        return receivePortalSuspended((ProtocolHandler.PortalSuspended) handler);

      case NO_DATA_MSG_ID:
        if (!(handler instanceof ProtocolHandler.NoData)) return null;
        return receiveNoData((ProtocolHandler.NoData) handler);

      case PARSE_COMPLETE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ParseComplete)) return null;
        return receiveParseComplete((ProtocolHandler.ParseComplete) handler);

      case BIND_COMPLETE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.BindComplete)) return null;
        return receiveBindComplete((ProtocolHandler.BindComplete) handler);

      case CLOSE_COMPLETE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.CloseComplete)) return null;
        return receiveCloseComplete((ProtocolHandler.CloseComplete) handler);

      case EMPTY_QUERY_MSG_ID:
        if (!(handler instanceof ProtocolHandler.EmptyQuery)) return null;
        return receiveEmptyQuery((ProtocolHandler.EmptyQuery) handler);

      case FUNCTION_RESULT_MSG_ID:
        if (!(handler instanceof ProtocolHandler.FunctionResult)) return null;
        return receiveFunctionResult(data, (ProtocolHandler.FunctionResult) handler);

      case ERROR_MSG_ID:
        if (!(handler instanceof ProtocolHandler.CommandError)) return null;
        return receiveError(data, (ProtocolHandler.CommandError) handler);

      case COMMAND_COMPLETE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.CommandComplete)) return null;
        return receiveCommandComplete(data, (ProtocolHandler.CommandComplete) handler);

      case READY_FOR_QUERY_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ReadyForQuery)) return null;
        return receiveReadyForQuery(data, (ProtocolHandler.ReadyForQuery) handler);

      default:
        throw new IOException("unsupported message type: " + (id & 0xff));
    }

  }

  private ProtocolHandler.Action receiveNotification(ByteBuf buffer, ProtocolHandler.Notification notificationHandler) throws IOException {

    int processId = buffer.readInt();
    String channelName = readCString(buffer, charset);
    String payload = readCString(buffer, charset);

    notificationHandler.notification(processId, channelName, payload);

    return ProtocolHandler.Action.Resume;
  }

  private ProtocolHandler.Action receiveNegotiation(ByteBuf buffer, ProtocolHandler.NegotiateProtocolVersion handler) throws IOException {

    int minorProtocol = buffer.readInt();
    int unsupportedParameterCount = buffer.readInt();

    String[] unsupportedParameters = new String[unsupportedParameterCount];
    for (int c = 0; c < unsupportedParameterCount; ++c) {
      unsupportedParameters[c] = readCString(buffer, charset);
    }

    return handler.negotiate(minorProtocol, asList(unsupportedParameters));
  }

  private ProtocolHandler.Action receiveAuthentication(ChannelHandlerContext ctx, ByteBuf buffer, ProtocolHandler.Authentication handler) throws IOException {

    ProtocolChannel protocolChannel = new ProtocolChannel(ctx.channel(), ctx, charset);

    int code = buffer.readInt();
    switch (code) {
      case 0:

        // Ok
        return handler.authenticated();

      case 2:

        // KerberosV5
        handler.authenticateKerberos(protocolChannel);
        return ProtocolHandler.Action.Resume;

      case 3:

        // Cleartext
        handler.authenticateClear(protocolChannel);
        return ProtocolHandler.Action.Resume;

      case 5:

        // MD5
        byte[] salt = new byte[4];
        buffer.readBytes(salt);

        handler.authenticateMD5(salt, protocolChannel);
        return ProtocolHandler.Action.Resume;

      case 6:

        // SCM Credential
        handler.authenticateSCM(protocolChannel);
        return ProtocolHandler.Action.Resume;

      case 7:

        // GSS
        handler.authenticateGSS(buffer, protocolChannel);
        return ProtocolHandler.Action.Resume;

      case 8:

        // GSS Continue
        handler.authenticateContinue(buffer, protocolChannel);
        return ProtocolHandler.Action.Resume;

      case 9:

        // SSPI
        handler.authenticateSSPI(buffer, protocolChannel);
        return ProtocolHandler.Action.Resume;

      default:
        throw new IOException("invalid authentication type");
    }
  }

  private ProtocolHandler.Action receiveBackendKeyData(ByteBuf buffer, ProtocolHandler.BackendKeyData handler) throws IOException {

    int processId = buffer.readInt();
    int secretKey = buffer.readInt();

    return handler.backendKeyData(processId, secretKey);
  }

  private ProtocolHandler.Action receiveParameterStatus(ByteBuf buffer, ProtocolHandler.ParameterStatus handler) throws IOException {

    String name = readCString(buffer, charset);
    String value = readCString(buffer, charset);

    // Watch for special parameters we care about...

    if ("client_encoding".equals(name)) {
      charset = Charset.forName(value);
    }

    return handler.parameterStatus(name, value);
  }

  private ProtocolHandler.Action receiveError(ByteBuf buffer, ProtocolHandler.CommandError handler) throws IOException {

    Notice notice = parseNotice(buffer, charset);

    return handler.error(notice);
  }

  private ProtocolHandler.Action receiveNotice(ByteBuf buffer, ProtocolHandler.ReportNotice handler) throws IOException {

    Notice notice = parseNotice(buffer, charset);

    return handler.notice(notice);
  }

  private ProtocolHandler.Action receiveParameterDescriptions(ByteBuf buffer, ProtocolHandler.ParameterDescriptions handler) throws IOException {

    int paramCount = buffer.readUnsignedShort();

    TypeRef[] paramTypes = new TypeRef[paramCount];

    for (int c = 0; c < paramCount; ++c) {

      paramTypes[c] = TypeOid.valueOf(buffer.readInt());
    }

    return handler.parameterDescriptions(paramTypes);
  }

  private ProtocolHandler.Action receiveRowDescription(ByteBuf buffer, ProtocolHandler.RowDescription handler) throws IOException {

    int fieldCount = buffer.readUnsignedShort();

    ResultField[] fields = new ResultField[fieldCount];

    for (int c = 0; c < fieldCount; ++c) {

      ResultField field = new ResultField(readCString(buffer, charset),
          buffer.readInt(),
          (short) buffer.readUnsignedShort(),
          TypeOid.valueOf(buffer.readInt()),
          buffer.readShort(),
          buffer.readInt(),
          FieldFormat.values()[buffer.readUnsignedShort()]);

      fields[c] = field;
    }

    return handler.rowDescription(fields);
  }

  private ProtocolHandler.Action receiveRowData(ByteBuf buffer, ProtocolHandler.DataRow handler) throws IOException {

    return handler.rowData(buffer);
  }

  private ProtocolHandler.Action receivePortalSuspended(ProtocolHandler.PortalSuspended handler) throws IOException {

    return handler.portalSuspended();
  }

  private ProtocolHandler.Action receiveNoData(ProtocolHandler.NoData handler) throws IOException {

    return handler.noData();
  }

  private ProtocolHandler.Action receiveCloseComplete(ProtocolHandler.CloseComplete handler) throws IOException {

    return handler.closeComplete();
  }

  private ProtocolHandler.Action receiveBindComplete(ProtocolHandler.BindComplete handler) throws IOException {

    return handler.bindComplete();
  }

  private ProtocolHandler.Action receiveParseComplete(ProtocolHandler.ParseComplete handler) throws IOException {

    return handler.parseComplete();
  }

  private ProtocolHandler.Action receiveEmptyQuery(ProtocolHandler.EmptyQuery handler) throws IOException {

    return handler.emptyQuery();
  }

  private ProtocolHandler.Action receiveFunctionResult(ByteBuf buffer, ProtocolHandler.FunctionResult handler) throws IOException {

    return handler.functionResult(buffer);
  }

  private ProtocolHandler.Action receiveCommandComplete(ByteBuf buffer, ProtocolHandler.CommandComplete handler) throws IOException {

    String commandTag = readCString(buffer, charset);

    String command;
    Long rowsAffected = null;
    Long insertedOid = null;

    try {
      int lastSpace = commandTag.lastIndexOf(' ');

      if (lastSpace != -1 && Character.isDigit(commandTag.charAt(lastSpace + 1))) {
        rowsAffected = Long.valueOf(commandTag.substring(lastSpace + 1));

        if (Character.isDigit(commandTag.charAt(lastSpace - 1))) {
          int nextToLastSpace = commandTag.lastIndexOf(' ', lastSpace - 1);
          insertedOid = Long.valueOf(commandTag.substring(nextToLastSpace + 1, lastSpace));
          command = commandTag.substring(0, nextToLastSpace);
        }
        else {
          command = commandTag.substring(0, lastSpace);
        }
      }
      else {
        command = commandTag;
      }
    }
    catch (NumberFormatException | StringIndexOutOfBoundsException e) {
      throw new IOException("Unrecognized command tag: " + commandTag);
    }

    return handler.commandComplete(command, rowsAffected, insertedOid);
  }

  private ProtocolHandler.Action receiveReadyForQuery(ByteBuf buffer, ProtocolHandler.ReadyForQuery handler) throws IOException {

    TransactionStatus previousTransactionStatus = transactionStatus;

    switch (buffer.readByte()) {
      case 'T':
        transactionStatus = Active;
        trace("[");
        break;
      case 'E':
        transactionStatus = Failed;
        trace("!");
        break;
      case 'I':
        transactionStatus = Idle;
        if (previousTransactionStatus != Idle) {
          trace("]");
        }
        break;
      default:
        throw new IllegalStateException("invalid transaction status");
    }

    return handler.readyForQuery(transactionStatus);
  }

  private Notice parseNotice(ByteBuf buffer, Charset charset) {

    Notice notice = new Notice();

    byte msgId;

    while ((msgId = buffer.readByte()) != 0) {

      switch (msgId) {
        case 'S':
          notice.setSeverity(readCString(buffer, charset));
          break;

        case 'C':
          notice.setCode(readCString(buffer, charset));
          break;

        case 'M':
          notice.setMessage(readCString(buffer, charset));
          break;

        case 'D':
          notice.setDetail(readCString(buffer, charset));
          break;

        case 'H':
          notice.setHint(readCString(buffer, charset));
          break;

        case 'P':
          notice.setPosition(readCString(buffer, charset));
          break;

        case 'W':
          notice.setWhere(readCString(buffer, charset));
          break;

        case 'F':
          notice.setFile(readCString(buffer, charset));
          break;

        case 'L':
          notice.setLine(readCString(buffer, charset));
          break;

        case 'R':
          notice.setRoutine(readCString(buffer, charset));
          break;

        case 's':
          notice.setSchema(readCString(buffer, charset));
          break;

        case 't':
          notice.setTable(readCString(buffer, charset));
          break;

        case 'c':
          notice.setColumn(readCString(buffer, charset));
          break;

        case 'd':
          notice.setDatatype(readCString(buffer, charset));
          break;

        case 'n':
          notice.setConstraint(readCString(buffer, charset));
          break;

        default:
          // Read and ignore
          readCString(buffer, charset);
          break;
      }

    }

    return notice;
  }

  private void flushTrace() {
    if (traceWriter == null) return;
    try {
      traceWriter.flush();
    }
    catch (IOException ignored) {
    }
  }

  private void trace(char dir, char id) {
    if (traceWriter == null) return;
    try {
      traceWriter.append(dir);
      traceWriter.append(id);
    }
    catch (IOException ignored) {
    }
  }

  private void trace(CharSequence code) {
    if (traceWriter == null) return;
    try {
      traceWriter.append(code);
    }
    catch (IOException ignored) {
    }
  }

}
