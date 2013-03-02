package com.impossibl.postgres;

import java.util.List;

import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Type;

public interface Context {

	StringCodec getStringCodec();

	Class<?> lookupInstanceType(Type type);
	Object createInstance(Class<?> type);

	void authenticatePlain();
	void authenticateMD5(byte[] salt);
	
	void setKeyData(int processId, int secretKey);

	void refreshType(int typeId);
	Type createTupleType(List<Field> fields);

	void restart(TransactionStatus txStatus);

	void setParameterDescriptions(List<Type> asList);
	Type getParameterDataType();

	void setResultType(Type type);
	Type getResultType();
	

	void setResultData(Object value);

	void commandComplete(String commandTag);

	void bindComplete();
	void closeComplete();

	void updateSystemParameter(String name, String value);
	void reportNotification(int processId, String channelName, String payload);
	void reportNotice(byte type, String value);
	void reportError(byte type, String value);

	void authenticated();

	Object getSetting(String string);

}
