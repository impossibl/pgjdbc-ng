package com.impossibl.postgres.mapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * PropertySetter for a bean using a reflection Method
 * 
 * @author kdubb
 *
 */
public class MethodPropertySetter implements PropertySetter {

	Method method;

	public MethodPropertySetter(Method method) {
		super();
		this.method = method;
	}

	@Override
	public void set(Object instance, Object value) {

		try {
			method.invoke(instance, value);
		}
		catch(IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
			// Ignore mapping errors (they shouldn't happen)
		}
	}

}
