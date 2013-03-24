package com.impossibl.postgres.types;

import java.util.Collection;

import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;


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
		return this;
	}

	@Override
	public void load(PgType.Row source, Collection<PgAttribute.Row> attrs, Registry registry) {
		
		super.load(source, attrs, registry);
		
		base = registry.loadType(source.arrayTypeId);
	}
	
}
