package com.impossibl.postgres.system;

import java.nio.charset.Charset;
import java.util.TimeZone;

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
	public TimeZone getTimeZone() {
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
	public Object getSetting(String name) {
		return base.getSetting(name);
	}

	@Override
	public Protocol getProtocol() {
		return base.getProtocol();
	}

}
