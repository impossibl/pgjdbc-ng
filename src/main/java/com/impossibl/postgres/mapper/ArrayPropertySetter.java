package com.impossibl.postgres.mapper;

/**
 * PropertySetter for an Object array using an index 
 * 
 * @author kdubb
 *
 */
public class ArrayPropertySetter implements PropertySetter {

	int propertyIndex;

	public ArrayPropertySetter(int idx) {
		this.propertyIndex = idx;
	}

	@Override
	public void set(Object instance, Object value) {

		Object[] array = (Object[]) instance;

		array[propertyIndex] = value;
	}

}
