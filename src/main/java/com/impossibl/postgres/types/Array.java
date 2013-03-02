package com.impossibl.postgres.types;

public class Array extends Type {

	private Type elementType;

	public Type getElementType() {
		return elementType;
	}

	public void setElementType(Type elementType) {
		this.elementType = elementType;
	}
	
}
