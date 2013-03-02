package com.impossibl.postgres;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public class BasicContext implements Context {
	
	private Map<String, Class<?>>  targetTypeMap;
	private StringCodec stringCodec;
	private Charset charset;
	private DataOutputStream out;
	private DataInputStream in;
	
	public BasicContext(Map<String, Class<?>> targetTypeMap, StringCodec stringCodec) {
		this.targetTypeMap = targetTypeMap;
		this.stringCodec = stringCodec;
	}

	public Class<?> lookupInstanceType(Type type) {
		
		return targetTypeMap.get(type.getName());
	}
	
	public Object createInstance(Class<?> type) {
		
		if(type == null)
			return new HashMap<String, Object>();
		
		try {
			return type.newInstance();
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		
	}

	public StringCodec getStringCodec() {
		return stringCodec;
	}

	public void refreshType(int typeId) {
	}

	@Override
	public void authenticatePlain() {
		
		Protocol.sendAuthenticate(out);
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
