package com.impossibl.postgres.types;

import java.util.Collection;

import com.impossibl.postgres.system.tables.PgType.Row;

/**
 * A database enumeration type.
 * 
 * @author kdubb
 *
 */
public class EnumerationType extends Type {

	Type textType;
	
	@Override
	public Type unwrap() {
		return textType;
	}

	@Override
	public PrimitiveType getPrimitiveType() {
		return textType.getPrimitiveType();
	}
	
	@Override
	public void load(Row source, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> attrs, Registry registry) {
		super.load(source, attrs, registry);
		textType = registry.loadType("text");
	}

}
