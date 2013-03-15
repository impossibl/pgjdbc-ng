package com.impossibl.postgres.mapper;

import java.util.Map;


/**
 * PropertySetter for a map using a name
 * 
 * @author kdubb
 *
 */
public class MapPropertySetter implements PropertySetter {

	String propertyName;

	public MapPropertySetter(String propertyName) {
		super();
		this.propertyName = propertyName;
	}

	@Override
	public void set(Object instance, Object value) {

		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) instance;

		map.put(propertyName, value);

	}

}
