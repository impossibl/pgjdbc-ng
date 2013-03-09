package com.impossibl.postgres.protocol;

import com.impossibl.postgres.types.Type;

public class ResultField {
	
	public enum Format {
		Text,
		Binary
	}
	
	public String name;
	public int relationId;
	public short relationAttributeIndex;
	public Type type;
	public short typeLength;
	public int typeModId;
	public Format format;
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(name);
		if(relationId != 0) {
			sb.append(String.format(" (%s:%d)", relationId, relationAttributeIndex));
		}
		sb.append(" : ");
		sb.append(type != null ? type.getName() : "<unknown>");
		return sb.toString();
	}
	
}
