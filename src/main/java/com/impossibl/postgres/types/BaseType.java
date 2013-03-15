package com.impossibl.postgres.types;

import static com.impossibl.postgres.system.procs.Procs.loadNamedBinaryCodec;
import static com.impossibl.postgres.system.procs.Procs.loadNamedTextCodec;

/**
 * A database primitive type
 * 
 * @author kdubb
 *
 */
public class BaseType extends Type {

	public BaseType() {
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, Codec binaryCodec, Codec textCodec) {
		super(id, name, length, alignment, category, delimeter, arrayType, binaryCodec, textCodec);
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, String procName) {
		super(id, name, length, alignment, category, delimeter, arrayType, loadNamedBinaryCodec(procName, null), loadNamedTextCodec(procName, null));
	}
	
}
