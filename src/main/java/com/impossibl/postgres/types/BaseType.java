package com.impossibl.postgres.types;

import static com.impossibl.postgres.system.procs.Procs.loadNamedBinaryIO;
import static com.impossibl.postgres.system.procs.Procs.loadNamerTextIO;

public class BaseType extends Type {

	public BaseType() {
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, BinaryIO binaryIO, TextIO textIO, int sqlType) {
		super(id, name, length, alignment, category, delimeter, arrayType, binaryIO, textIO, sqlType);
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, String procName, int sqlType) {
		super(id, name, length, alignment, category, delimeter, arrayType, loadNamedBinaryIO(procName, null), loadNamerTextIO(procName, null), sqlType);
	}
	
}
