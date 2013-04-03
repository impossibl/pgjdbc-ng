package com.impossibl.postgres.system;

import java.nio.charset.Charset;
import java.util.TimeZone;

import com.impossibl.postgres.datetime.DateTimeFormat;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public interface Context {
	
	Registry getRegistry();
	
	TimeZone getTimeZone();
	
	Charset getCharset();
	
	DateTimeFormat getDateFormatter();
	DateTimeFormat getTimeFormatter();
	DateTimeFormat getTimestampFormatter();

	Class<?> lookupInstanceType(Type type);

	void refreshType(int typeId);
	void refreshRelationType(int relationId);	

	Object getSetting(String name);
	<T> T getSetting(String name, Class<T> type);
	boolean isSettingEnabled(String name);
	
	Protocol getProtocol();

}
