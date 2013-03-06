package com.impossibl.postgres;

import com.impossibl.postgres.codecs.DateTimeCodec;
import com.impossibl.postgres.codecs.StringCodec;
import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.ProtocolHandler;
import com.impossibl.postgres.protocol.ProtocolV30;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public interface Context {

	DataInputStream getInputStream();
	DataOutputStream getOutputStream();
	
	StringCodec getStringCodec();
	DateTimeCodec getDateTimeCodec();

	Class<?> lookupInstanceType(Type type);

	void refreshType(int typeId);

	Object getSetting(String string);
	
	void setKeyData(int processId, int secretKey);
	void updateSystemParameter(String name, String value);
	
	void reportNotification(int processId, String channelName, String payload);
	void reportNotice(byte type, String value);
	void reportError(Error error);

	ProtocolV30 lockProtocol(ProtocolHandler handler);
	void unlockProtocol();

}
