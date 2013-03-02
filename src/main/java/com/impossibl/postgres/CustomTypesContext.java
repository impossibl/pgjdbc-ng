package com.impossibl.postgres;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;

public class CustomTypesContext implements Context {
	
	private Context parent;
	private Map<String, Class<?>>  targetTypeMap;
	
	public CustomTypesContext(Context parent) {
		this.parent = parent;
		this.targetTypeMap = new HashMap<String, Class<?>>();
	}

	public Class<?> lookupInstanceType(Type type) {
		
		Class<?> targetType = targetTypeMap.get(type);
		if(targetType == null) {
		
			if(parent != null) {
				return parent.lookupInstanceType(type);
			}
		}
		
		return targetType;
	}
	
	public Object createInstance(Class<?> type) {
		 return parent.createInstance(type);
	}

	public StringCodec getStringCodec() {
		return parent.getStringCodec();
	}

	public void refreshType(int typeId) {
		parent.refreshType(typeId);
	}

	@Override
	public void authenticatePlain() {
		
	}

	@Override
	public void authenticateMD5(byte[] salt) {
	}

	@Override
	public void setKeyData(int processId, int secretKey) {
	}

	@Override
	public Type createTupleType(List<Field> fields) {
		return null;
	}

	@Override
	public void restart(TransactionStatus txStatus) {
		
	}

	@Override
	public void setParameterDescriptions(List<Type> asList) {
	}

	@Override
	public Type getParameterDataType() {
		return null;
	}

	@Override
	public void setResultType(Type type) {
	}

	@Override
	public Type getResultType() {
		return null;
	}

	@Override
	public void setResultData(Object value) {
	}

	@Override
	public void commandComplete(String commandTag) {
	}

	@Override
	public void bindComplete() {
	}

	@Override
	public void closeComplete() {
	}

	@Override
	public void updateSystemParameter(String name, String value) {
	}

	@Override
	public void reportNotification(int processId, String channelName, String payload) {
	}

	@Override
	public void reportNotice(byte type, String value) {
	}

	@Override
	public void reportError(byte type, String value) {
	}

	@Override
	public void authenticated() {
	}

}
