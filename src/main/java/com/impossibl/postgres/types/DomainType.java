package com.impossibl.postgres.types;

import java.util.Collection;
import java.util.Map;

import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;

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
	private int numberOfDimensions;
	private String defaultValue;

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
	
	public int getNumberOfDimensions() {
		return numberOfDimensions;
	}
	
	public void setNumberOfDimensions(int numberOfDimensions) {
		this.numberOfDimensions = numberOfDimensions;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}
	
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public Type unwrap() {
		return base.unwrap();
	}

	@Override
	public void load(PgType.Row source, Collection<PgAttribute.Row> attrs, Registry registry) {
		
		super.load(source, attrs, registry);
		
		base = registry.loadType(source.domainBaseTypeId);
		nullable = source.domainNotNull;
		modifiers = base.getModifierParser().parse(source.domainTypeMod);
		numberOfDimensions = source.domainDimensions;
		defaultValue = source.domainDefault != null ? source.domainDefault : "";
	}
	
}
