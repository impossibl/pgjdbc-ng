package com.impossibl.postgres.protocol;

import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public class TypeRef {
	
	Object val;
	
	public Type get() {
		if(val instanceof Locator) {
			val = ((Locator) val).locate();
		}
		return (Type) val;
	}
	
	private TypeRef(Object val) {
		this.val = val;
	}
	
	public static TypeRef from(int typeId, Registry registry) {
		return new TypeRef(new Locator(typeId, registry));
	}
	
	public static TypeRef from(Type type) {
		return new TypeRef(type);
	}
	
}


class Locator {
	
	public int typeId;
	public Registry registry;
	
	public Locator(int typeId, Registry registry) {
		this.typeId = typeId;
		this.registry = registry;
	}

	public Type locate() {
		return registry.loadType(typeId);
	}

	@Override
	public String toString() {
		return Integer.toString(typeId);
	}
	
}
