package com.impossibl.postgres.data;

import com.impossibl.postgres.types.CompositeType;

public class Record {
	
	CompositeType type;
	Object[] values;

	public Record(CompositeType type, Object[] values) {
		this.type = type;
		this.values = values;
	}

	public CompositeType getType() {
		return type;
	}

	public void setType(CompositeType type) {
		this.type = type;
	}

	public Object[] getValues() {
		return values;
	}

	public void setValues(Object[] values) {
		this.values = values;
	}

}
