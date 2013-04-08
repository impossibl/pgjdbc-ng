package com.impossibl.postgres.system;

import java.util.Map;

import com.impossibl.postgres.types.Type;

public class TypeMapContext extends DecoratorContext {
	
	Map<String, Class<?>> typeMap;

	public TypeMapContext(Context base, Map<String, Class<?>> typeMap) {
		super(base);
		this.typeMap = typeMap;
	}

	@Override
	public Class<?> lookupInstanceType(Type type) {
		Class<?> instanceType = typeMap.get(type.getName());
		if(instanceType == null)
			instanceType = super.lookupInstanceType(type);
		return instanceType;
	}
	
	
}
