package com.impossibl.postgres;

import java.util.List;

import com.impossibl.postgres.codecs.StringCodec;
import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.Field;
import com.impossibl.postgres.protocol.TransactionStatus;
import com.impossibl.postgres.types.TupleType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public interface Context {

	DataInputStream getInputStream();
	DataOutputStream getOutputStream();
	
	StringCodec getStringCodec();

	Class<?> lookupInstanceType(Type type);

	void refreshType(int typeId);
	TupleType createTupleType(List<Field> fields);

	void restart(TransactionStatus txStatus);

	Object getSetting(String string);
	
	void setKeyData(int processId, int secretKey);
	void updateSystemParameter(String name, String value);
	
	void reportNotification(int processId, String channelName, String payload);
	void reportNotice(byte type, String value);
	void reportError(Error error);

}
