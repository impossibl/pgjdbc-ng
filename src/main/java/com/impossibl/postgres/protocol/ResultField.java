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
	public Type type;
	public short typeLength;
	public int typeModifier;
	public Format format;
	
	public ResultField(String name, int relationId, short relationAttributeIndex, Type type, short typeLength, int typeModifier, Format format) {
		super();
		this.name = name;
		this.relationId = relationId;
		this.relationAttributeNumber = relationAttributeIndex;
		this.type = type;
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
		sb.append(type != null ? type.getName() : "<unknown>");
		return sb.toString();
	}
	
}
