package com.impossibl.postgres.utils;

import java.lang.reflect.Array;

public class Factory {

	public static <T> T createInstance(Class<T> type, int sizeIfArray) {
		
		try {
			
			if(type.isArray()) {
				return type.cast(Array.newInstance(type.getComponentType(), sizeIfArray));
			}
			
			return type.newInstance();
		}
		catch (InstantiationException e) {
			throw new RuntimeException(e);
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		
	}

}
