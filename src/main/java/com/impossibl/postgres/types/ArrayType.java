package com.impossibl.postgres.types;

import java.lang.reflect.Array;
import java.util.Map;

import com.impossibl.postgres.protocol.ResultField.Format;



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

	public Format getParameterFormat() {
		
		return elementType.getParameterFormat();
	}
	
	public Format getResultFormat() {
		
		return elementType.getResultFormat();
	}

	public Type unwrap() {
		return elementType;
	}

}
