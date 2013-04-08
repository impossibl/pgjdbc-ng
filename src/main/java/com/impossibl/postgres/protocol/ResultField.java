package com.impossibl.postgres.protocol;

import com.impossibl.postgres.types.Type;

public class ResultField {
	
	public enum Format {
		Text,
		Binary
	}
	
	public String name;
	public int relationId;
	public short relationAttributeNumber;
	public TypeRef typeRef;
	public short typeLength;
	public int typeModifier;
	public Format format;
	
	public ResultField(String name, int relationId, short relationAttributeIndex, Type type, short typeLength, int typeModifier, Format format) {
		super();
		this.name = name;
		this.relationId = relationId;
		this.relationAttributeNumber = relationAttributeIndex;
		this.typeRef = TypeRef.from(type);
		this.typeLength = typeLength;
		this.typeModifier = typeModifier;
		this.format = format;
	}

	public ResultField(String name, int relationId, short relationAttributeIndex, TypeRef typeRef, short typeLength, int typeModifier, Format format) {
		super();
		this.name = name;
		this.relationId = relationId;
		this.relationAttributeNumber = relationAttributeIndex;
		this.typeRef = typeRef;
		this.typeLength = typeLength;
		this.typeModifier = typeModifier;
		this.format = format;
	}

	public ResultField() {
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
