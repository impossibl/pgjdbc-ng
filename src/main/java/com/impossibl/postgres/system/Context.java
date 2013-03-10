package com.impossibl.postgres.system;

import java.nio.charset.Charset;
import java.util.TimeZone;

import org.jboss.netty.channel.Channel;

import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public interface Context {
	
	Registry getRegistry();
	
	Channel getChannel();

	Charset getCharset();
	TimeZone getTimeZone();

	Class<?> lookupInstanceType(Type type);

	void refreshType(int typeId);

	Object getSetting(String string);
	
	void setKeyData(int processId, int secretKey);
	void updateSystemParameter(String name, String value);
	
	void reportNotification(int processId, String channelName, String payload);
	void reportNotice(byte type, String value);
	void reportError(Error error);

	Protocol getProtocol();

}
