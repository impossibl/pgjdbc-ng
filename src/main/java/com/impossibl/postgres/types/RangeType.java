package com.impossibl.postgres.types;

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
		return base.unwrap();
	}

}
