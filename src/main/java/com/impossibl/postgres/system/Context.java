package com.impossibl.postgres.system;

import java.nio.charset.Charset;
import java.util.TimeZone;

import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public interface Context {
	
	Registry getRegistry();
	
	Charset getCharset();
	TimeZone getTimeZone();

	Class<?> lookupInstanceType(Type type);

	void refreshType(int typeId);

	Object getSetting(String name);
	
	Protocol getProtocol();

}
