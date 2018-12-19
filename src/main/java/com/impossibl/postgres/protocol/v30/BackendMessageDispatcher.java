package com.impossibl.postgres.protocol.v30;

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.LocatingTypeRef;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Action;
import com.impossibl.postgres.protocol.v30.ProtocolHandler.Authentication.GSSStage;
import com.impossibl.postgres.system.BasicContext;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.TypeRef;

import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Failed;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static com.impossibl.postgres.utils.ByteBufs.readCString;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import static java.util.Arrays.asList;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

class BackendMessageDispatcher {

  private static final Logger LOGGER = Logger.getLogger(BackendMessageDispatcher.class.getName());

  // Backend messages
  static private final byte NEGOTIATE_PROTOCOL_VERSION_ID = 'B';
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

  static boolean isReadyForQuery(byte id) {
    return id == READY_FOR_QUERY_MSG_ID;
  }

  /*
   *
   * Message dispatching & parsing
   */

  static Action backendDispatch(BasicContext context, Channel channel, ServerConnection serverConnection,
                                byte id, ByteBuf data,
                                ProtocolHandler handler) throws IOException {

    switch (id) {
      case NOTIFICATION_MSG_ID:
        if (serverConnection.getNotificationHandler() != null) {
          receiveNotification(context, data, serverConnection.getNotificationHandler());
        }
        return Action.Resume;

      case NOTICE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ReportNotice)) return Action.Resume;
        return receiveNotice(context, data, (ProtocolHandler.ReportNotice) handler);

      case NEGOTIATE_PROTOCOL_VERSION_ID:
        if (!(handler instanceof ProtocolHandler.NegotiateProtocolVersion)) return null;
        return receiveNegotiation(context, data, (ProtocolHandler.NegotiateProtocolVersion) handler);

      case AUTHENTICATION_MSG_ID:
        if (!(handler instanceof ProtocolHandler.Authentication)) return null;
        return receiveAuthentication(context, channel, data, (ProtocolHandler.Authentication) handler);

      case BACKEND_KEY_MSG_ID:
        if (!(handler instanceof ProtocolHandler.BackendKeyData)) return null;
        return receiveBackendKeyData(data, (ProtocolHandler.BackendKeyData) handler);

      case PARAMETER_STATUS_MSG_ID:
        if (!(handler instanceof ProtocolHandler.BackendKeyData)) {
          return receiveParameterStatus(context, data, null);
        }
        return receiveParameterStatus(context, data, (ProtocolHandler.ParameterStatus) handler);

      case PARAMETER_DESC_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ParameterDescriptions)) return null;
        return receiveParameterDescriptions(context, data, (ProtocolHandler.ParameterDescriptions) handler);

      case ROW_DESC_MSG_ID:
        if (!(handler instanceof ProtocolHandler.RowDescription)) return null;
        return receiveRowDescription(context, data, (ProtocolHandler.RowDescription) handler);

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
        return receiveError(context, data, (ProtocolHandler.CommandError) handler);

      case COMMAND_COMPLETE_MSG_ID:
        if (!(handler instanceof ProtocolHandler.CommandComplete)) return null;
        return receiveCommandComplete(context, data, (ProtocolHandler.CommandComplete) handler);

      case READY_FOR_QUERY_MSG_ID:
        if (!(handler instanceof ProtocolHandler.ReadyForQuery)) return null;
        return receiveReadyForQuery(serverConnection, data, (ProtocolHandler.ReadyForQuery) handler);

      default:
        throw new IOException("unsupported message type: " + (id & 0xff));
    }

  }

  private static void receiveNotification(Context context, ByteBuf buffer, ProtocolHandler.Notification notificationHandler) throws IOException {

    int processId = buffer.readInt();
    String channelName = readCString(buffer, context.getCharset());
    String payload = readCString(buffer, context.getCharset());

    notificationHandler.notification(processId, channelName, payload);
  }

  private static Action receiveNegotiation(Context context, ByteBuf buffer, ProtocolHandler.NegotiateProtocolVersion handler) throws IOException {

    int minorProtocol = buffer.readInt();
    int unsupportedParameterCount = buffer.readInt();

    String[] unsupportedParameters = new String[unsupportedParameterCount];
    for (int c = 0; c < unsupportedParameterCount; ++c) {
      unsupportedParameters[c] = readCString(buffer, context.getCharset());
    }

    return handler.negotiate(minorProtocol, asList(unsupportedParameters));
  }

  private static Action receiveAuthentication(Context context, Channel channel, ByteBuf buffer, ProtocolHandler.Authentication handler) throws IOException {

    ProtocolChannel protocolChannel = new ProtocolChannel(channel, context.getCharset());

    int code = buffer.readInt();
    switch (code) {
      case 0:

        // Ok
        return handler.authenticated();

      case 2:

        // KerberosV5
        handler.authenticateKerberos(context, protocolChannel);
        return Action.Resume;

      case 3:

        // Cleartext
        handler.authenticateClear(context, protocolChannel);
        return Action.Resume;

      case 4:

        // Crypt
        handler.authenticateCrypt(context, protocolChannel);
        return Action.Resume;

      case 5:

        // MD5
        byte[] salt = new byte[4];
        buffer.readBytes(salt);

        handler.authenticateMD5(context, salt, protocolChannel);
        return Action.Resume;

      case 6:

        // SCM Credential
        handler.authenticateSCM(context, protocolChannel);
        return Action.Resume;

      case 7:

        // GSS
        handler.authenticateGSS(context, GSSStage.Initialize, protocolChannel);
        return Action.Resume;

      case 8:

        // GSS Continue
        handler.authenticateGSS(context, GSSStage.Continue, protocolChannel);
        return Action.Resume;

      case 9:

        // SSPI
        handler.authenticateSSPI(context, protocolChannel);
        return Action.Resume;

      default:
        throw new IOException("invalid authentication type");
    }
  }

  private static Action receiveBackendKeyData(ByteBuf buffer, ProtocolHandler.BackendKeyData handler) throws IOException {

    int processId = buffer.readInt();
    int secretKey = buffer.readInt();

    LOGGER.finest("BACKEND KEY DATA: " + processId + ": XXXXXXXX");

    return handler.backendKeyData(processId, secretKey);
  }

  private static Action receiveParameterStatus(BasicContext context, ByteBuf buffer, ProtocolHandler.ParameterStatus handler) throws IOException {

    String name = readCString(buffer, context.getCharset());
    String value = readCString(buffer, context.getCharset());

    LOGGER.finest("PARAMETER STATUS: " + name + " = " + value);

    if (handler != null) {
      return handler.parameterStatus(name, value);
    }
    else {
      context.updateSystemParameter(name, value);
      return Action.Resume;
    }
  }

  private static Action receiveError(Context context, ByteBuf buffer, ProtocolHandler.CommandError handler) throws IOException {

    Notice notice = parseNotice(buffer, context.getCharset());

    LOGGER.finest("ERROR: " + notice.getCode() + ": " + notice.getMessage());

    return handler.error(notice);
  }

  private static Action receiveNotice(Context context, ByteBuf buffer, ProtocolHandler.ReportNotice handler) throws IOException {

    Notice notice = parseNotice(buffer, context.getCharset());

    LOGGER.finest(notice.getSeverity() + ": " + notice.getCode() + ": " + notice.getMessage());

    return handler.notice(notice);
  }

  private static Action receiveParameterDescriptions(Context context, ByteBuf buffer, ProtocolHandler.ParameterDescriptions handler) throws IOException {

    int paramCount = buffer.readUnsignedShort();

    TypeRef[] paramTypes = new TypeRef[paramCount];

    for (int c = 0; c < paramCount; ++c) {

      int paramTypeId = buffer.readInt();

      paramTypes[c] = LocatingTypeRef.from(paramTypeId, context.getRegistry());
    }

    LOGGER.finest("PARAM-DESC: " + paramCount);

    return handler.parameterDescriptions(paramTypes);
  }

  private static Action receiveRowDescription(Context context, ByteBuf buffer, ProtocolHandler.RowDescription handler) throws IOException {

    Registry registry = context.getRegistry();

    int fieldCount = buffer.readUnsignedShort();

    ResultField[] fields = new ResultField[fieldCount];

    for (int c = 0; c < fieldCount; ++c) {

      ResultField field = new ResultField(readCString(buffer, context.getCharset()),
          buffer.readInt(),
          (short) buffer.readUnsignedShort(),
          LocatingTypeRef.from(buffer.readInt(), registry),
          buffer.readShort(),
          buffer.readInt(),
          FieldFormat.values()[buffer.readUnsignedShort()]);

      fields[c] = field;
    }

    LOGGER.finest("ROW-DESC: " + fieldCount);

    return handler.rowDescription(fields);
  }

  private static Action receiveRowData(ByteBuf buffer, ProtocolHandler.DataRow handler) throws IOException {

    LOGGER.finest("DATA");

    return handler.rowData(buffer);
  }

  private static Action receivePortalSuspended(ProtocolHandler.PortalSuspended handler) throws IOException {

    LOGGER.finest("SUSPEND");

    return handler.portalSuspended();
  }

  private static Action receiveNoData(ProtocolHandler.NoData handler) throws IOException {

    LOGGER.finest("NO-DATA");

    return handler.noData();
  }

  private static Action receiveCloseComplete(ProtocolHandler.CloseComplete handler) throws IOException {

    LOGGER.finest("CLOSE-COMP");

    return handler.closeComplete();
  }

  private static Action receiveBindComplete(ProtocolHandler.BindComplete handler) throws IOException {

    LOGGER.finest("BIND-COMP");

    return handler.bindComplete();
  }

  private static Action receiveParseComplete(ProtocolHandler.ParseComplete handler) throws IOException {

    LOGGER.finest("PARSE-COMP");

    return handler.parseComplete();
  }

  private static Action receiveEmptyQuery(ProtocolHandler.EmptyQuery handler) throws IOException {

    LOGGER.finest("EMPTY");

    return handler.emptyQuery();
  }

  private static Action receiveFunctionResult(ByteBuf buffer, ProtocolHandler.FunctionResult handler) throws IOException {

    LOGGER.finest("FUNCTION-RES");

    return handler.functionResult(buffer);
  }

  private static Action receiveCommandComplete(Context context, ByteBuf buffer, ProtocolHandler.CommandComplete handler) throws IOException {

    String commandTag = readCString(buffer, context.getCharset());

    String command = null;
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

    LOGGER.finest("COMPLETE: " + commandTag);

    return handler.commandComplete(command, rowsAffected, insertedOid);
  }

  private static Action receiveReadyForQuery(ServerConnection serverConnection, ByteBuf buffer, ProtocolHandler.ReadyForQuery handler) throws IOException {

    TransactionStatus txStatus;

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

    serverConnection.setTransactionStatus(txStatus);

    return handler.readyForQuery(txStatus);
  }

  private static Notice parseNotice(ByteBuf buffer, Charset charset) {

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

}
