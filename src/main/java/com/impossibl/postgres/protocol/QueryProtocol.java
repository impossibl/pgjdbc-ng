package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.utils.Factory.createInstance;
import static java.util.Arrays.asList;
import static org.apache.commons.beanutils.BeanUtils.setProperty;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.TupleType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class QueryProtocol<T> extends CommandProtocol {
	
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
	private TupleType resultsType;
	private List<T> results;
	

	public static <T> QueryProtocol<T> get(Context context, Class<T> rowType) {
		return new QueryProtocol<>(context, rowType);
	}
	
	public QueryProtocol(Context context, Class<T> rowType) {
		super(context);
		this.rowType = rowType;
		parameterTypes = new ArrayList<Type>();
		results = new ArrayList<T>();
	}
	
	public List<T> getResults() {
		return results;
	}

	public void query(String query) throws IOException {
		
		Message msg = new Message(QUERY_MSG_ID);
		
		msg.writeCString(query);
		
		sendMessage(msg);
	}
	
	public void queryParse(String stmtName, String query, List<Type> paramTypes) throws IOException {
		
		Message msg = new Message(PARSE_MSG_ID);
		
		msg.writeCString(stmtName != null ? stmtName : "");
		msg.writeCString(query);

		msg.writeShort(paramTypes.size());
		for(Type paramType : paramTypes) {
			msg.writeInt(paramType.getId());
		}
				
		sendMessage(msg);
	}
	
	public void queryBind(String portalName, String stmtName, List<Object> paramValues) throws IOException {
		
		Message msg = new Message(BIND_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeCString(stmtName != null ? stmtName : "");

		loadParams(msg, parameterTypes, paramValues);
		
		//Binary format for all results fields
		msg.writeShort(1);
		msg.writeShort(1);		

		sendMessage(msg);				
	}
	
	public void queryExecute(String portalName, int maxRows) throws IOException {
		
		Message msg = new Message(EXECUTE_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeInt(maxRows);
		
		sendMessage(msg);
	}

	public void describe(char targetType, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(targetType);
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
	public void close(char targetType, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(targetType);
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
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


	protected void parameterDescriptions(List<Type> paramTypes) throws IOException {
		this.parameterTypes = paramTypes;
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
	

	protected void rowDescription(TupleType tupleType) throws IOException {
		this.resultsType = tupleType;
	}

	private void receiveRowDescription(DataInputStream in) throws IOException {
		
		short fieldCount = in.readShort();

		Field[] fields = new Field[fieldCount];

		for (int c = 0; c < fieldCount; ++c) {

			Field field = new Field();
			field.name = in.readCString();
			field.relationId = in.readInt();
			field.attributeIndex = in.readShort();
			field.typeId = in.readInt();
			field.typeLength = in.readShort();
			field.typeModId = in.readInt();
			field.formatCode = in.readShort();

			fields[c] = field;
		}
		
		TupleType tupleType = context.createTupleType(asList(fields));
		
		rowDescription(tupleType);
	}
	
	protected void rowData(T rowInstance) throws IOException {
		results.add(rowInstance);
	}
	
	private void receiveRowData(DataInputStream in) throws IOException {
		
		T rowInstance = createInstance(rowType);

		int itemCount = in.readShort();
		
		for (int c = 0; c < itemCount; ++c) {

			Attribute attribute = resultsType.getAttribute(c);

			Type attributeType = attribute.type;
			Object attributeVal = null;
			
			attributeVal = attributeType.getBinaryIO().decoder.decode(attributeType, in, context);

			set(rowInstance, attribute.name, attributeVal);
		}

		rowData(rowInstance);
	}
	
	@SuppressWarnings("unchecked")
	protected void set(Object instance, String name, Object value) {
		
		if (instance instanceof Map) {
			
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
	

	protected void portalSuspended() {
	}
	
	private void receivePortalSuspended(DataInputStream in) {
		portalSuspended();
	}
	
	
	protected void noData() {
	}
	
	private void receiveNoData(DataInputStream in) {
		noData();
	}
	
	
	protected void closeComplete() {
	}
	
	private void receiveCloseComplete(DataInputStream in) {
		closeComplete();
	}


	protected void bindComplete() {
	}
	
	private void receiveBindComplete(DataInputStream in) {
		bindComplete();
	}

	
	protected void parseComplete() {
	}
	
	private void receiveParseComplete(DataInputStream in) {
		parseComplete();
	}

	
	protected void emptyQuery() {
	}
	
	private void receiveEmptyQuery(DataInputStream in) {
		emptyQuery();
	}
		
}
