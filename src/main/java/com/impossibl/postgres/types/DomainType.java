package com.impossibl.postgres.types;

import java.util.Map;

/**
 * A database domain type.
 * 
 * @author kdubb
 *
 */
public class DomainType extends Type {
	
	private Type base;
	private boolean nullable;
	private Map<String, Object> modifiers;

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

	public Map<String, Object> getModifiers() {
		return modifiers;
	}
	
	public void setModifiers(Map<String, Object> modifiers) {
		this.modifiers = modifiers;
	}
	
	public Type unwrap() {
		return base.unwrap();
	}

}
