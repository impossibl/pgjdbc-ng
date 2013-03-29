package com.impossibl.postgres.jdbc;

public class ArrayUtils {

	public static int getDimensions(Object array) {
		return getDimensions(array.getClass());
	}
	
	public static int getDimensions(Class<?> cls) {
		return cls.getName().lastIndexOf('[') + 1;
	}

}
