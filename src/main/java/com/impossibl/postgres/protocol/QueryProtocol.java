package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.utils.Factory.createInstance;
import static java.util.Arrays.asList;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.system.procs.Arrays;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class QueryProtocol<T> extends CommandProtocol {
	
	enum Status {
		Completed,
		Suspended
	}
	
	
	static class ResultField {
		
		enum Format {
			Text,
			Binary
		}
		
		public String name;
		public int relationId;
		public short relationAttributeIndex;
		public Type type;
		public short typeLength;
		public int typeModId;
		public Format format;
		
		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(name);
			if(relationId != 0) {
				sb.append(String.format(" (%s:%d)", relationId, relationAttributeIndex));
			}
			sb.append(" : ");
			sb.append(type != null ? type.getName() : "<unknown>");
			return sb.toString();
		}
		
	}
	
	
	//Frontend messages
	private static final byte QUERY_MSG_ID 					= 'Q';	
	private static final byte PARSE_MSG_ID 					= 'P';
	private static final byte BIND_MSG_ID 					= 'B';
	private static final byte DESCRIBE_MSG_ID 			= 'D';
	private static final byte EXECUTE_MSG_ID 				= 'E';

	//Backend messages
	private static final byte PARAMETER_DESC_MSG_ID		= 't';
	private static final byte ROW_DESC_MSG_ID					= 'T';
	private static final byte ROW_DATA_MSG_ID					= 'D';
	private static final byte PORTAL_SUSPENDED_MSG_ID	= 's';
	private static final byte NO_DATA_MSG_ID					= 'n';
	private static final byte EMPTY_QUERY_MSG_ID			= 'I';
	private static final byte PARSE_COMPLETE_MSG_ID 	= '1';
	private static final byte BIND_COMPLETE_MSG_ID 		= '2';
	private static final byte CLOSE_COMPLETE_MSG_ID 	= '3';
	
	
	private Class<T> rowType;
	private List<Type> parameterTypes;
	private List<ResultField> resultFields;
	private List<T> results;
	private Status status;
	

	public static <T> QueryProtocol<T> get(Context context, Class<T> rowType) {
		return new QueryProtocol<>(context, rowType);
	}
	
	public QueryProtocol(Context context, Class<T> rowType) {
		super(context);
		this.rowType = rowType;
		parameterTypes = new ArrayList<>();
		results = new ArrayList<>();
		resultFields = Collections.emptyList();
	}
	
	@Override
	public boolean isRunComplete() {
		return super.isRunComplete() || status != null;
	}

	public List<T> getResults() {
		return results;
	}

	public Status getStatus() {
		return status;
	}

	public void sendQuery(String query) throws IOException {
		
		Message msg = new Message(QUERY_MSG_ID);
		
		msg.writeCString(query);
		
		sendMessage(msg);
	}
	
	public void sendParse(String stmtName, String query, List<Type> paramTypes) throws IOException {
		
		Message msg = new Message(PARSE_MSG_ID);
		
		msg.writeCString(stmtName != null ? stmtName : "");
		msg.writeCString(query);

		msg.writeShort(paramTypes.size());
		for(Type paramType : paramTypes) {
			msg.writeInt(paramType.getId());
		}
				
		sendMessage(msg);
	}
	
	public void sendBind(String portalName, String stmtName, List<Object> paramValues) throws IOException {
		
		Message msg = new Message(BIND_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeCString(stmtName != null ? stmtName : "");

		loadParams(msg, parameterTypes, paramValues);
		
		//Binary format for all results fields
		msg.writeShort(1);
		msg.writeShort(1);		

		sendMessage(msg);				
	}
	
	public void sendExecute(String portalName, int maxRows) throws IOException {
		
		Message msg = new Message(EXECUTE_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeInt(maxRows);
		
		sendMessage(msg);
	}

	public void sendDescribe(char targetType, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(targetType);
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
	public void sendClose(char targetType, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(targetType);
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
	protected void parameterDescriptions(List<Type> paramTypes) throws IOException {
		this.parameterTypes = paramTypes;
	}

	protected void rowDescription(List<ResultField> resultFields) throws IOException {
		this.resultFields = resultFields;
	}

	protected void rowData(T rowInstance) throws IOException {
		results.add(rowInstance);
	}
	
	protected void portalSuspended() {
		status = Status.Suspended;
	}
	
	protected void noData() {
		resultFields = Collections.emptyList();
	}
	
	protected void closeComplete() {
	}
	
	protected void bindComplete() {
	}
	
	protected void parseComplete() {
	}
	
	protected void emptyQuery() {
		status = Status.Completed;
	}
	
	@Override
	protected void commandComplete(String commandTag) {
		super.commandComplete(commandTag);
		status = Status.Completed;
	}

	
	/*
	 * 
	 * Message dispatching & parsing
	 * 
	 */

	
	@Override
	public boolean dispatch(DataInputStream in, byte msgId) throws IOException {
		
		if(super.dispatch(in, msgId))
			return true;
		
		switch(msgId) {
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
		}
		
		return false;
	}


	private void receiveParameterDescriptions(DataInputStream in) throws IOException {

		short paramCount = in.readShort();

		Type[] paramTypes = new Type[paramCount];
		
		for(int c=0; c < paramCount; ++c) {
			
			int paramTypeId = in.readInt();
			paramTypes[c] = Registry.loadType(paramTypeId);
		}
		
		parameterDescriptions(asList(paramTypes));
	}
	

	private void receiveRowDescription(DataInputStream in) throws IOException {
		
		short fieldCount = in.readShort();

		ResultField[] fields = new ResultField[fieldCount];

		for (int c = 0; c < fieldCount; ++c) {

			ResultField field = new ResultField();
			field.name = in.readCString();
			field.relationId = in.readInt();
			field.relationAttributeIndex = in.readShort();
			field.type = Registry.loadType(in.readInt());
			field.typeLength = in.readShort();
			field.typeModId = in.readInt();
			field.format = ResultField.Format.values()[in.readShort()];

			fields[c] = field;
		}
		
		rowDescription(asList(fields));
	}
	
	private void receiveRowData(DataInputStream in) throws IOException {

		if(resultFields.isEmpty())
			throw new IllegalStateException("No result data expected");
		
		int itemCount = in.readShort();

		Reader reader = new InputStreamReader(in);
		
		T rowInstance = createInstance(rowType, itemCount);

		for (int c = 0; c < itemCount; ++c) {

			ResultField field = resultFields.get(c);

			Type fieldType = field.type;
			Object fieldVal = null;
			
			switch(field.format) {
			case Text:
				fieldVal = fieldType.getTextIO().decoder.decode(fieldType, reader, context);
				break;
				
			case Binary:
				fieldVal = fieldType.getBinaryIO().decoder.decode(fieldType, in, context);
				break;
			}

			set(rowInstance, c, field.name, fieldVal);
		}

		rowData(rowInstance);
	}
	
	@SuppressWarnings("unchecked")
	protected void set(Object instance, int idx, String name, Object value) throws IOException {
		
		if(Arrays.is(instance)) {
			
			Arrays.set(instance, idx, value);
			return;
		}
		else if (instance instanceof Map) {
			
			((Map<Object,Object>)instance).put(name, value);
			return;
		}
		else {
			
			try {
				
				java.lang.reflect.Field field;
				
				if((field = instance.getClass().getField(name)) != null) {
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
	

	private void receivePortalSuspended(DataInputStream in) {
		portalSuspended();
	}
	
	
	private void receiveNoData(DataInputStream in) {
		noData();
	}
	
	
	private void receiveCloseComplete(DataInputStream in) {
		closeComplete();
	}


	private void receiveBindComplete(DataInputStream in) {
		bindComplete();
	}

	
	private void receiveParseComplete(DataInputStream in) {
		parseComplete();
	}

	
	private void receiveEmptyQuery(DataInputStream in) {
		emptyQuery();
	}
		
}
