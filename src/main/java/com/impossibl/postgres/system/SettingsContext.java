package com.impossibl.postgres.system;

import java.util.HashMap;
import java.util.Map;

public class SettingsContext extends DecoratorContext {
	
	Map<String, Object> settings;

	public SettingsContext(Context context, Map<String, Object> settings) {
		super(context);
		this.settings = settings;
	}

	public SettingsContext(Context context) {
		super(context);
		settings = new HashMap<>();
	}

	public Object getSetting(String name) {
		Object res = settings.get(name);
		if(res != null)
			return res;
		return super.getSetting(name);
	}
	
	public void setSetting(String name, Object value) {
		settings.put(name, value);
	}

}
