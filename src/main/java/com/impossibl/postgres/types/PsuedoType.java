package com.impossibl.postgres.types;

import java.util.Collection;

import com.impossibl.postgres.system.tables.PgType.Row;

/**
 * A database psuedo type.
 * 
 * @author kdubb
 *
 */
public class PsuedoType extends Type {
	
	@Override
	public void load(Row source, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> attrs, Registry registry) {
		super.load(source, attrs, registry);
	}
	
}
