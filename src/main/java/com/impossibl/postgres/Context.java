package com.impossibl.postgres;

import java.util.List;

import com.impossibl.postgres.codecs.StringCodec;
import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.types.Type;

public interface Context {

	StringCodec getStringCodec();

	Protocol getProtocol();

	Class<?> lookupInstanceType(Type type);
	Object createInstance(Class<?> type);

	void refreshType(int typeId);
	Tuple createTupleType(List<Field> fields);

	void restart(TransactionStatus txStatus);
	
	void authenticated();
	void setKeyData(int processId, int secretKey);
	void updateSystemParameter(String name, String value);
	void reportNotification(int processId, String channelName, String payload);
	void reportNotice(byte type, String value);
	void reportError(byte type, String value);

	Object getSetting(String string);

}
