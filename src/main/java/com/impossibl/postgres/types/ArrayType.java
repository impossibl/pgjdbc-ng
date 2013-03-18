package com.impossibl.postgres.types;

import static com.impossibl.postgres.types.PrimitiveType.Array;

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

	public Type unwrap() {
		return elementType;
	}

	@Override
	public PrimitiveType getPrimitiveType() {
		return Array;
	}

}
