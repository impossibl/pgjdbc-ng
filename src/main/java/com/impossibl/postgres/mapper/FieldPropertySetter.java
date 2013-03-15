package com.impossibl.postgres.mapper;

import java.lang.reflect.Field;


/**
 * PropertySetter for a bean using a reflection Field
 * 
 * @author kdubb
 *
 */
public class FieldPropertySetter implements PropertySetter {

	Field field;

	public FieldPropertySetter(Field field) {
		super();
		this.field = field;
	}

	@Override
	public void set(Object instance, Object value) {

		try {
			field.set(instance, value);
		}
		catch(IllegalArgumentException | IllegalAccessException e) {
			// Ignore mapping errors (they shouldn't happen)
		}
	}

}
