package com.impossibl.postgres.system;

import java.nio.charset.Charset;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public interface Context {
	
	Registry getRegistry();
	
	Charset getCharset();
	DateTimeZone getTimeZone();
	DateTimeFormatter getDateFormatter();
	DateTimeFormatter getTimeFormatter();
	DateTimeFormatter getTimestampFormatter();

	Class<?> lookupInstanceType(Type type);

	void refreshType(int typeId);

	Object getSetting(String name);
	
	Protocol getProtocol();



}
