package com.impossibl.postgres.protocol;

import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

public class ResultField {
	
	public enum Format {
		Text,
		Binary
	}
	
	public static class TypeLocator {
		
		public int typeId;
		public Registry registry;
		
		public TypeLocator(int typeId, Registry registry) {
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
	
	public String name;
	public int relationId;
	public short relationAttributeNumber;
	public Object typeRef;
	public short typeLength;
	public int typeModifier;
	public Format format;
	
	public ResultField(String name, int relationId, short relationAttributeIndex, Type type, short typeLength, int typeModifier, Format format) {
		super();
		this.name = name;
		this.relationId = relationId;
		this.relationAttributeNumber = relationAttributeIndex;
		this.typeRef = type;
		this.typeLength = typeLength;
		this.typeModifier = typeModifier;
		this.format = format;
	}

	public ResultField(String name, int relationId, short relationAttributeIndex, TypeLocator typeLocator, short typeLength, int typeModifier, Format format) {
		super();
		this.name = name;
		this.relationId = relationId;
		this.relationAttributeNumber = relationAttributeIndex;
		this.typeRef = typeLocator;
		this.typeLength = typeLength;
		this.typeModifier = typeModifier;
		this.format = format;
	}

	public ResultField() {
	}
	
	public Type getType() {
		if(typeRef instanceof TypeLocator) {
			typeRef = ((TypeLocator) typeRef).locate(); 
		}
		return (Type) typeRef;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		if(relationId != 0) {
			sb.append(String.format(" (%s:%d)", relationId, relationAttributeNumber));
		}
		sb.append(" : ");
		sb.append(typeRef != null ? typeRef.toString() : "<unknown>");
		return sb.toString();
	}
	
}
