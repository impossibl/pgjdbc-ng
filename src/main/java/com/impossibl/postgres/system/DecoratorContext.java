package com.impossibl.postgres.system;

import java.nio.charset.Charset;
import java.util.TimeZone;

import com.impossibl.postgres.datetime.DateTimeFormat;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public class DecoratorContext implements Context {
	
	Context base;

	public DecoratorContext(Context base) {
		super();
		this.base = base;
	}

	@Override
	public Registry getRegistry() {
		return base.getRegistry();
	}

	@Override
	public TimeZone getTimeZone() {
		return base.getTimeZone();
	}

	@Override
	public Charset getCharset() {
		return base.getCharset();
	}

	@Override
	public DateTimeFormat getDateFormatter() {
		return base.getDateFormatter();
	}

	@Override
	public DateTimeFormat getTimeFormatter() {
		return base.getTimeFormatter();
	}

	@Override
	public DateTimeFormat getTimestampFormatter() {
		return base.getTimestampFormatter();
	}

	@Override
	public Class<?> lookupInstanceType(Type type) {
		return base.lookupInstanceType(type);
	}

	@Override
	public void refreshType(int typeId) {
		base.refreshType(typeId);
	}

	@Override
	public void refreshRelationType(int relationId) {
		base.refreshRelationType(relationId);
	}

	@Override
	public Object getSetting(String name) {
		return base.getSetting(name);
	}

	@Override
	public <T> T getSetting(String name, Class<T> type) {
		return base.getSetting(name, type);
	}

	@Override
	public boolean isSettingEnabled(String name) {
		return base.isSettingEnabled(name);
	}
	
	@Override
	public Protocol getProtocol() {
		return base.getProtocol();
	}

}
