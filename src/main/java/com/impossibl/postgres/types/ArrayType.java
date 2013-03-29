package com.impossibl.postgres.types;

import java.lang.reflect.Array;



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
	
	public Class<?> getJavaType() {
		
		return Array.newInstance(elementType.getJavaType(), 0).getClass();
		
	}

	public Type unwrap() {
		return elementType;
	}

}
