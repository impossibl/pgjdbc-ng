package com.impossibl.postgres.types;

public class TupleType extends CompositeType {

	public TupleType() {
		super();
	}

	public TupleType(int id, String name, Type arrayType, int sqlType) {
		super(id, name, arrayType, "tuple_", sqlType);
	}

}
