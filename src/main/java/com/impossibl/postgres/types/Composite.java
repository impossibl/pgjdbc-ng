package com.impossibl.postgres.types;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;

public class Composite extends Type {
	
	public static class Attribute {
		
		public String name;
		public Type type;
		
		@Override
		public String toString() {
			return name + " : " + type;
		}
		
	}
	
	private List<Attribute> attributes;

	public Attribute getAttribute(int idx) {
		return attributes.get(idx);
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}

	@Override
	public void load(PgType.Row pgType, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> pgAttrs) {
		
		super.load(pgType, pgAttrs);
		
		Attribute[] attributes = new Attribute[pgAttrs.size()];
		
		for(PgAttribute.Row pgAttr : pgAttrs) {
			
			Attribute attr = new Attribute();
			attr.name = pgAttr.name;
			attr.type = Registry.loadType(pgAttr.typeId);
			
			attributes[pgAttr.number-1] = attr;
		}
		
		this.attributes = Arrays.asList(attributes);
	}

}
