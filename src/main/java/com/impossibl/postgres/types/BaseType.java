package com.impossibl.postgres.types;

import static com.impossibl.postgres.system.procs.Procs.loadNamedBinaryCodec;
import static com.impossibl.postgres.system.procs.Procs.loadNamedTextCodec;

import java.util.Collection;

import com.impossibl.postgres.system.tables.PgType.Row;

/**
 * A database primitive type
 * 
 * @author kdubb
 *
 */
public class BaseType extends Type {
	
	public BaseType() {
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, Codec binaryCodec, Codec textCodec) {
		super(id, name, length, alignment, category, delimeter, arrayTypeId, binaryCodec, textCodec);
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, String procName) {
		super(id, name, length, alignment, category, delimeter, arrayTypeId, loadNamedBinaryCodec(procName, null), loadNamedTextCodec(procName, null));
	}
	
	@Override
	public void load(Row source, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> attrs, Registry registry) {
		super.load(source, attrs, registry);
	}
	
}
