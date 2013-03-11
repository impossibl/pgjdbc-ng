package com.impossibl.postgres.types;

/**
 * A database domain type.
 * 
 * @author kdubb
 *
 */
public class DomainType extends Type {
	
	private Type base;
	private boolean nullable;

	public Type getBase() {
		return base;
	}
	public void setBase(Type base) {
		this.base = base;
	}
	public boolean isNullable() {
		return nullable;
	}
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

}
