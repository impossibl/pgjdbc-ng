package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Failed;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;
import static com.impossibl.postgres.utils.Factory.createInstance;
import static java.util.Arrays.asList;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.procs.Arrays;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class ProtocolV30 implements Protocol {

	private static Logger logger = Logger.getLogger(ProtocolV30.class.getName());

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

	Context context;
	TransactionStatus txStatus;
	ProtocolHandler handler;

	public ProtocolV30(Context context) {
		this.context = context;
		this.txStatus = Idle;
	}

	@Override
	public void close() {
		context.unlockProtocol();
	}

	@Override
	public TransactionStatus getTransactionStatus() {
		return txStatus;
	}

	@Override
	public void run(ProtocolHandler handler) throws IOException {

		try {

			this.handler = handler;

			while (!handler.isComplete() && receive())
				;

		}
		finally {

			this.handler = null;
		}

	}

	@Override
	public void sendStartup(Map<String, Object> params) throws IOException {

		Message msg = new Message((byte) 0);

		// Version
		msg.writeShort(3);
		msg.writeShort(0);

		// Name=Value pairs
		for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
			msg.writeCString(paramEntry.getKey());
			msg.writeCString(paramEntry.getValue().toString());
		}

		msg.writeByte(0);

		sendMessage(msg);
	}

	@Override
	public void sendPassword(String password) throws IOException {

		Message msg = new Message(PASSWORD_MSG_ID);

		msg.writeCString(password);

		sendMessage(msg);
	}

	@Override
	public void sendQuery(String query) throws IOException {

		Message msg = new Message(QUERY_MSG_ID);

		msg.writeCString(query);

		sendMessage(msg);
	}

	@Override
	public void sendParse(String stmtName, String query, List<Type> paramTypes) throws IOException {

		Message msg = new Message(PARSE_MSG_ID);

		msg.writeCString(stmtName != null ? stmtName : "");
		msg.writeCString(query);

		msg.writeShort(paramTypes.size());
		for (Type paramType : paramTypes) {
			msg.writeInt(paramType.getId());
		}

		sendMessage(msg);
	}

	@Override
	public void sendBind(String portalName, String stmtName, List<Type> parameterTypes, List<Object> parameterValues) throws IOException {

		Message msg = new Message(BIND_MSG_ID);

		msg.writeCString(portalName != null ? portalName : "");
		msg.writeCString(stmtName != null ? stmtName : "");

		loadParams(msg, parameterTypes, parameterValues);

		// Binary format for all results fields
		msg.writeShort(1);
		msg.writeShort(1);

		sendMessage(msg);
	}

	@Override
	public void sendDescribe(ServerObject target, String targetName) throws IOException {

		Message msg = new Message(DESCRIBE_MSG_ID);

		msg.writeByte(target.getId());
		msg.writeCString(targetName != null ? targetName : "");

		sendMessage(msg);
	}

	@Override
	public void sendExecute(String portalName, int maxRows) throws IOException {

		Message msg = new Message(EXECUTE_MSG_ID);

		msg.writeCString(portalName != null ? portalName : "");
		msg.writeInt(maxRows);

		sendMessage(msg);
	}

	@Override
	public void sendFunctionCall(int functionId, List<Type> paramTypes, List<Object> paramValues) throws IOException {

		Message msg = new Message(FUNCTION_CALL_MSG_ID);

		msg.writeInt(functionId);

		loadParams(msg, paramTypes, paramValues);

		msg.writeShort(1);

		sendMessage(msg);
	}

	@Override
	public void sendClose(ServerObject target, String targetName) throws IOException {

		Message msg = new Message(CLOSE_MSG_ID);

		msg.writeByte(target.getId());
		msg.writeCString(targetName != null ? targetName : "");

		sendMessage(msg);
	}

	@Override
	public void sendFlush() throws IOException {
		sendMessage(FLUSH_MSG_ID, 0);
	}

	@Override
	public void sendSync() throws IOException {
		sendMessage(SYNC_MSG_ID, 0);
	}

	@Override
	public void sendTerminate() throws IOException {
		sendMessage(TERMINATE_MSG_ID, 0);
	}

	protected void loadParams(DataOutputStream out, List<Type> paramTypes, List<Object> paramValues) throws IOException {

		// Binary format for all parameters
		out.writeShort(1);
		out.writeShort(1);

		// Values for each parameter
		if (paramTypes == null) {
			out.writeShort(0);
		}
		else {
			out.writeShort(paramTypes.size());
			for (int c = 0; c < paramTypes.size(); ++c) {

				Type paramType = paramTypes.get(c);
				Object paramValue = paramValues.get(c);

				paramType.getBinaryIO().encoder.encode(paramType, out, paramValue, context);
			}
		}
	}

	protected byte[] serialize(Type type, Object value) throws IOException {

		// OPTI: do this without allocating new byte streams

		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream dataStream = new DataOutputStream(byteStream);

		type.getBinaryIO().encoder.encode(type, dataStream, value, context);

		return byteStream.toByteArray();
	}

	protected void sendMessage(Message msg) throws IOException {

		DataOutputStream out = context.getOutputStream();

		ByteArrayOutputStream data = msg.getData();

		if (msg.getId() != 0)
			out.writeByte(msg.getId());

		out.writeInt(data.size() + 4);
		out.write(data.toByteArray());

		out.flush();
	}

	protected void sendMessage(byte msgId, int dataLength) throws IOException {

		DataOutputStream out = context.getOutputStream();

		out.writeByte(msgId);

		out.writeInt(dataLength + 4);
	}

	@Override
	public Object parseRowData(DataInputStream in, List<ResultField> resultFields, Class<?> rowType) throws IOException {

		int itemCount = in.readShort();

		Reader reader = new InputStreamReader(in);

		Object rowInstance = createInstance(rowType, itemCount);

		for (int c = 0; c < itemCount; ++c) {

			ResultField field = resultFields.get(c);

			Type fieldType = field.type;
			Object fieldVal = null;

			switch (field.format) {
			case Text:
				fieldVal = fieldType.getTextIO().decoder.decode(fieldType, reader, context);
				break;

			case Binary:
				fieldVal = fieldType.getBinaryIO().decoder.decode(fieldType, in, context);
				break;
			}

			setField(rowInstance, c, field.name, fieldVal);
		}

		return rowInstance;
	}

	@SuppressWarnings("unchecked")
	protected void setField(Object instance, int idx, String name, Object value) throws IOException {

		if (Arrays.is(instance)) {

			Arrays.set(instance, idx, value);
			return;
		}
		else if (instance instanceof Map) {

			((Map<Object, Object>) instance).put(name, value);
			return;
		}
		else {

			try {

				java.lang.reflect.Field field;

				if ((field = instance.getClass().getField(name)) != null) {
					field.set(instance, value);
					return;
				}

			}
			catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e1) {

				try {
					setProperty(instance, name.toString(), value);
					return;
				}
				catch (IllegalAccessException | InvocationTargetException e) {
				}

			}
		}

		throw new IllegalStateException("invalid poperty name/index");
	}

	@Override
	public Object parseResultData(DataInputStream in, Type resultType) throws IOException {

		Object value = null;

		int length = in.readInt();

		long start = in.getCount();

		if (length != -1) {

			value = resultType.getBinaryIO().decoder.decode(resultType, in, context);
		}

		if (length == (in.getCount() - start)) {
			throw new IOException("invalid result length");
		}

		return value;
	}

	/*
	 * 
	 * Message dispatching & parsing
	 */

	protected boolean receive() throws IOException {

		DataInputStream in = context.getInputStream();

		byte msgId = in.readByte();

		long msgStart = in.getCount();

		long msgLength = in.readInt();

		try {

			dispatch(in, msgId);

			return true;
		}
		finally {
			// Swallow leftover bytes in the event
			// the message dispatch failed
			long leftover = msgLength - (in.getCount() - msgStart);
			if (leftover > 0) {
				in.skip(leftover);
			}
		}

	}

	protected boolean dispatch(DataInputStream in, byte msgId) throws IOException {

		switch (msgId) {
		case AUTHENTICATION_MSG_ID:
			receiveAuthentication(in);
			return true;

		case BACKEND_KEY_MSG_ID:
			receiveBackendKeyData(in);
			return true;

		case PARAMETER_DESC_MSG_ID:
			receiveParameterDescriptions(in);
			return true;

		case ROW_DESC_MSG_ID:
			receiveRowDescription(in);
			return true;

		case ROW_DATA_MSG_ID:
			receiveRowData(in);
			return true;

		case PORTAL_SUSPENDED_MSG_ID:
			receivePortalSuspended(in);
			return true;

		case NO_DATA_MSG_ID:
			receiveNoData(in);
			return true;

		case PARSE_COMPLETE_MSG_ID:
			receiveParseComplete(in);
			return true;

		case BIND_COMPLETE_MSG_ID:
			receiveBindComplete(in);
			return true;

		case CLOSE_COMPLETE_MSG_ID:
			receiveCloseComplete(in);
			return true;

		case EMPTY_QUERY_MSG_ID:
			receiveEmptyQuery(in);
			return true;

		case FUNCTION_RESULT_MSG_ID:
			receiveFunctionResult(in);
			return true;

		case ERROR_MSG_ID:
			receiveError(in);
			return true;

		case NOTICE_MSG_ID:
			receiveNotice(in);
			return true;

		case NOTIFICATION_MSG_ID:
			receiveNotification(in);
			return true;

		case COMMAND_COMPLETE_MSG_ID:
			receiveCommandComplete(in);
			return true;

		case PARAMETER_STATUS_MSG_ID:
			receiveParameterStatus(in);
			return true;

		case READY_FOR_QUERY_MSG_ID:
			receiveReadyForQuery(in);
			return true;
		}

		return false;
	}

	private void receiveAuthentication(DataInputStream in) throws IOException {

		int code = in.readInt();
		switch (code) {
		case 0:

			// Ok
			handler.authenticated(this);
			return;

		case 2:

			// KerberosV5
			handler.authenticateKerberos(this);
			break;

		case 3:

			// Cleartext
			handler.authenticateClear(this);
			return;

		case 4:

			// Crypt
			handler.authenticateCrypt(this);
			return;

		case 5:

			// MD5
			byte[] salt = new byte[4];
			in.readFully(salt);

			handler.authenticateMD5(this, salt);

			return;

		case 6:

			// SCM Credential
			handler.authenticateSCM(this);
			break;

		case 7:

			// GSS
			handler.authenticateGSS(this);
			break;

		case 8:

			// GSS Continue
			handler.authenticateGSSCont(this);
			break;

		case 9:

			// SSPI
			handler.authenticateSSPI(this);
			break;

		}

		throw new UnsupportedOperationException("invalid authentication type");
	}

	private void receiveBackendKeyData(DataInputStream in) throws IOException {

		int processId = in.readInt();
		int secretKey = in.readInt();

		handler.backendKeyData(processId, secretKey);
	}

	private void receiveError(DataInputStream in) throws IOException {

		Error error = new Error();

		byte msgId;

		while ((msgId = in.readByte()) != 0) {

			switch (msgId) {
			case 'S':
				error.severity = Error.Severity.valueOf(in.readCString());
				break;

			case 'C':
				error.code = in.readCString();
				break;

			case 'M':
				error.message = in.readCString();
				break;

			case 'D':
				error.detail = in.readCString();
				break;

			case 'H':
				error.hint = in.readCString();
				break;

			case 'P':
				error.position = Integer.parseInt(in.readCString());
				break;

			case 'F':
				error.file = in.readCString();
				break;

			case 'L':
				error.line = Integer.parseInt(in.readCString());
				break;

			case 'R':
				error.routine = in.readCString();
				break;

			default:
				// Read and ignore
				in.readCString();
				break;
			}

		}

		logger.finest("ERROR: " + error.message);

		handler.error(error);
	}

	private void receiveParameterDescriptions(DataInputStream in) throws IOException {

		short paramCount = in.readShort();

		Type[] paramTypes = new Type[paramCount];

		for (int c = 0; c < paramCount; ++c) {

			int paramTypeId = in.readInt();
			paramTypes[c] = context.getRegistry().loadType(paramTypeId);
		}

		logger.finest("PARAM-DESC: " + paramCount);

		handler.parametersDescription(asList(paramTypes));
	}

	private void receiveRowDescription(DataInputStream in) throws IOException {

		short fieldCount = in.readShort();

		ResultField[] fields = new ResultField[fieldCount];

		for (int c = 0; c < fieldCount; ++c) {

			ResultField field = new ResultField();
			field.name = in.readCString();
			field.relationId = in.readInt();
			field.relationAttributeIndex = in.readShort();
			field.type = context.getRegistry().loadType(in.readInt());
			field.typeLength = in.readShort();
			field.typeModId = in.readInt();
			field.format = ResultField.Format.values()[in.readShort()];

			fields[c] = field;
		}

		logger.finest("ROW-DESC: " + fieldCount);

		handler.rowDescription(asList(fields));
	}

	private void receiveRowData(DataInputStream in) throws IOException {
		logger.finest("DATA");
		handler.rowData(this, in);
	}

	private void receivePortalSuspended(DataInputStream in) throws IOException {
		logger.finest("SUSPEND");
		handler.portalSuspended();
	}

	private void receiveNoData(DataInputStream in) throws IOException {
		logger.finest("NO-DATA");
		handler.noData();
	}

	private void receiveCloseComplete(DataInputStream in) throws IOException {
		logger.finest("CLOSE-COMP");
		handler.closeComplete();
	}

	private void receiveBindComplete(DataInputStream in) throws IOException {
		logger.finest("BIND-COMP");
		handler.bindComplete();
	}

	private void receiveParseComplete(DataInputStream in) throws IOException {
		logger.finest("PARSE-COMP");
		handler.parseComplete();
	}

	private void receiveEmptyQuery(DataInputStream in) throws IOException {
		logger.finest("EMPTY");
		handler.emptyQuery();
	}

	private void receiveFunctionResult(DataInputStream in) throws IOException {

		logger.finest("FUNCTION-RES");

		handler.functionResult(in);
	}

	private void receiveCommandComplete(DataInputStream in) throws IOException {

		String commandTag = in.readCString();

		String[] parts = commandTag.split(" ");

		String command = parts[0];
		Integer rowsAffected = null;
		Integer oid = null;

		switch (command) {

		case "INSERT":

			if (parts.length == 3) {

				oid = Integer.parseInt(parts[1]);
				rowsAffected = Integer.parseInt(parts[2]);
			}
			else {
				throw new IOException("error parsing command tag");
			}

			break;

		case "SELECT":
		case "UPDATE":
		case "DELETE":
		case "MOVE":
		case "FETCH":

			if (parts.length == 2) {

				rowsAffected = Integer.parseInt(parts[1]);
			}
			else {
				throw new IOException("error parsing command tag");
			}

			break;

		case "COPY":

			if (parts.length == 1) {

				// Nothing to parse but accepted
			}
			else if (parts.length == 2) {

				rowsAffected = Integer.parseInt(parts[1]);
			}
			else {
				throw new IOException("error parsing command tag");
			}

			break;

		default:
			throw new IOException("error parsing command tag");
		}

		logger.finest("COMPLETE: " + commandTag);

		handler.commandComplete(command, rowsAffected, oid);
	}

	protected void receiveNotification(DataInputStream in) throws IOException {

		int processId = in.readInt();
		String channelName = in.readCString();
		String payload = in.readCString();

		logger.finest("NOTIFY: " + processId + " - " + channelName + " - " + payload);

		handler.notification(processId, channelName, payload);
	}

	private void receiveNotice(DataInputStream in) throws IOException {

		byte type;

		while ((type = in.readByte()) != 0) {

			String value = in.readCString();

			logger.finest("NOTICE: " + type + " - " + value);

			context.reportNotice(type, value);

		}

	}

	private void receiveParameterStatus(DataInputStream in) throws IOException {

		String name = in.readCString();
		String value = in.readCString();

		context.updateSystemParameter(name, value);
	}

	private void receiveReadyForQuery(DataInputStream in) throws IOException {

		TransactionStatus txStatus;

		switch (in.readByte()) {
		case 'T':
			txStatus = Active;
			break;
		case 'F':
			txStatus = Failed;
			break;
		case 'I':
			txStatus = Idle;
			break;
		default:
			throw new IllegalStateException("invalid transaction status");
		}

		logger.finest("READY: " + txStatus);

		handler.ready(txStatus);
	}

}
