package com.impossibl.postgres.system;

import java.nio.charset.Charset;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

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
	public Charset getCharset() {
		return base.getCharset();
	}

	@Override
	public DateTimeFormatter getDateFormatter() {
		return base.getDateFormatter();
	}

	@Override
	public DateTimeFormatter getTimeFormatter() {
		return base.getTimeFormatter();
	}

	@Override
	public DateTimeFormatter getTimestampFormatter() {
		return base.getTimestampFormatter();
	}

	@Override
	public DateTimeZone getTimeZone() {
		return base.getTimeZone();
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
