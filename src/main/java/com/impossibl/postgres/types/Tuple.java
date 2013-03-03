package com.impossibl.postgres.types;

public class Tuple extends Composite {

	public Tuple() {
		super();
	}

	public Tuple(int id, String name, Type arrayType, int sqlType) {
		super(id, name, arrayType, "tuple_", sqlType);
	}

}
