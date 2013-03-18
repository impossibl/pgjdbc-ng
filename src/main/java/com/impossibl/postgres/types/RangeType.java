package com.impossibl.postgres.types;

import static com.impossibl.postgres.types.PrimitiveType.Range;

/**
 * A database range type.
 * 
 * @author kdubb
 *
 */
public class RangeType extends Type {

	Type base;
	
	public Type getBase() {
		return base;
	}
	
	public void setBase(Type base) {
		this.base = base;
	}

	public Type unwrap() {
		//TODO implement as return base.unwrap();
		return this;
	}

	@Override
	public PrimitiveType getPrimitiveType() {
		return Range;
	}

}
