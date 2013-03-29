package com.impossibl.postgres.types;

import java.lang.reflect.Array;
import java.util.Map;



/**
 * A database array type.
 * 
 * @author kdubb
 *
 */
public class ArrayType extends Type {

	private Type elementType;

	public Type getElementType() {
		return elementType;
	}

	public void setElementType(Type elementType) {
		this.elementType = elementType;
	}
	
	public Class<?> getJavaType(Map<String, Class<?>> customizations) {
		
		return Array.newInstance(elementType.getJavaType(customizations), 0).getClass();
		
	}

	public Type unwrap() {
		return elementType;
	}

}
