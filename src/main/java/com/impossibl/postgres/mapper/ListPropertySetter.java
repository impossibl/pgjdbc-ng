package com.impossibl.postgres.mapper;

import java.util.List;


/**
 * PropertySetter for a List based on an index
 * 
 * @author kdubb
 *
 */
public class ListPropertySetter implements PropertySetter {

	int propertyIndex;

	public ListPropertySetter(int idx) {
		this.propertyIndex = idx;
	}

	@Override
	public void set(Object instance, Object value) {

		@SuppressWarnings("unchecked")
		List<Object> list = (List<Object>) instance;

		fill(list, propertyIndex + 1);

		list.set(propertyIndex, value);

	}

	void fill(List<Object> list, int requiredSize) {

		for(int c = list.size(); c < requiredSize; ++c) {
			list.add(null);
		}

	}

}
