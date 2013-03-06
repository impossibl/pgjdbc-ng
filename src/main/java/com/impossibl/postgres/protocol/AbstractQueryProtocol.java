package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.utils.Factory.createInstance;
import static java.util.Arrays.asList;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.system.procs.Arrays;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class AbstractQueryProtocol extends CommandProtocol {
	
	private static final Logger logger = Logger.getLogger(AbstractQueryProtocol.class.getName());
	
	public enum Target {
		
		Statement	('S'),
		Portal		('P');
		
		byte id;
		Target(char id) { this.id = (byte)id; }
		public byte getId() { return id; }
	}
	
	
	//Frontend messages
	private static final byte QUERY_MSG_ID 					= 'Q';	
	private static final byte PARSE_MSG_ID 					= 'P';
	private static final byte BIND_MSG_ID 					= 'B';
	private static final byte DESCRIBE_MSG_ID 			= 'D';
	private static final byte EXECUTE_MSG_ID 				= 'E';
	private static final byte CLOSE_MSG_ID					= 'C';

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
	
	
	public AbstractQueryProtocol(Context context) {
		super(context);
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
	
	public void sendBind(String portalName, String stmtName, List<Type> parameterTypes, List<Object> parameterValues) throws IOException {
		
		Message msg = new Message(BIND_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeCString(stmtName != null ? stmtName : "");

		loadParams(msg, parameterTypes, parameterValues);
		
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

	public void sendDescribe(Target target, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(target.getId());
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
	public void sendClose(Target target, String targetName) throws IOException {
		
		Message msg = new Message(CLOSE_MSG_ID);
		
		msg.writeByte(target.getId());
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}

	
	protected void parameterDescriptions(List<Type> paramTypes) {
		throw new UnsupportedOperationException();
	}
	
	protected void rowDescription(List<ResultField> resultFields) {
		throw new UnsupportedOperationException();
	}

	protected void rowData(Object rowInstance) throws IOException {
		throw new UnsupportedOperationException();
	}

	protected void portalSuspended() {
		throw new UnsupportedOperationException();
	}

	protected void noData() {
		throw new UnsupportedOperationException();
	}

	protected void parseComplete() {
		throw new UnsupportedOperationException();
	}

	protected void bindComplete() {
		throw new UnsupportedOperationException();
	}

	protected void closeComplete() {
		throw new UnsupportedOperationException();
	}

	protected void emptyQuery() {
		throw new UnsupportedOperationException();
	}
	
	protected List<ResultField> getResultFields() {
		throw new UnsupportedOperationException();
	}
	
	protected Class<?> getRowType() {
		throw new UnsupportedOperationException();
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
		
		logger.finest("PARAM-DESC: " + paramCount);
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

		logger.finest("ROW-DESC: " + fieldCount);
	}
	
	private void receiveRowData(DataInputStream in) throws IOException {

		List<ResultField> resultFields = getResultFields();
		if(resultFields.isEmpty())
			throw new IllegalStateException("No result data expected");
		
		int itemCount = in.readShort();

		Reader reader = new InputStreamReader(in);
		
		Object rowInstance = createInstance(getRowType(), itemCount);

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
		
		logger.finest("DATA");
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
		logger.finest("SUSPEND");
	}
	
	
	private void receiveNoData(DataInputStream in) {
		noData();
		logger.finest("NO-DATA");
	}
	
	
	private void receiveCloseComplete(DataInputStream in) {
		closeComplete();
		logger.finest("CLOSE-COMP");
	}


	private void receiveBindComplete(DataInputStream in) {
		bindComplete();
		logger.finest("BIND-COMP");
	}

	
	private void receiveParseComplete(DataInputStream in) {
		parseComplete();
		logger.finest("PARSE-COMP");
	}

	
	private void receiveEmptyQuery(DataInputStream in) {
		emptyQuery();
		logger.finest("EMPTY");
	}

}
