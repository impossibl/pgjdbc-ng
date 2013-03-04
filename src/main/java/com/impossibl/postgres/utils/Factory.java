package com.impossibl.postgres.utils;

public class Factory {

	public static <T> T createInstance(Class<T> type) {
		
		try {
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
