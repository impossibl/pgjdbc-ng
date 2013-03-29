package com.impossibl.postgres.types;

import static com.impossibl.postgres.system.procs.Procs.loadNamedBinaryCodec;
import static com.impossibl.postgres.system.procs.Procs.loadNamedTextCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;


/**
 * A database composite type.
 * 
 * @author kdubb
 *
 */
public class CompositeType extends Type {

	/**
	 *	An attribute of the composite type.
	 */
	public static class Attribute {

		public int number;
		public String name;
		public Type type;
		public boolean nullable;
		public boolean autoIncrement;
		public boolean hasDefault;
		public Map<String, Object> typeModifiers;

		@Override
		public String toString() {
			return name + " : " + type;
		}

	}

	private List<Attribute> attributes;

	public CompositeType(int id, String name, int arrayTypeId, String procName) {
		super(id, name, null, null, Category.Composite, ',', arrayTypeId, loadNamedBinaryCodec(procName, null), loadNamedTextCodec(procName, null));
	}

	public CompositeType(int id, String name, int arrayTypeId) {
		this(id, name, arrayTypeId, "record_");
	}

	public CompositeType() {
	}

	public Attribute getAttribute(int number) {
		
		//First try the obvious
		if(number > 0 && attributes.get(number-1).number == number) {
			return attributes.get(number-1);
		}
		
		//Now search all
		for(int c=0, sz=attributes.size(); c < sz; ++c) {
			Attribute attr = attributes.get(c);
			if(attr.number == number) {
				return attr;
			}
		}
		
		return null;
	}

	public List<Attribute> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Attribute> attributes) {
		this.attributes = attributes;
	}

	@Override
	public Class<?> getJavaType(Map<String, Class<?>> customizations) {

		Class<?> type = customizations.get(getName());
		if(type == null) {
			type = super.getJavaType(customizations);
		}

		return type;
	}

	@Override
	public void load(PgType.Row pgType, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> pgAttrs, Registry registry) {

		super.load(pgType, pgAttrs, registry);

		if(pgAttrs == null) {
			
			attributes = Collections.emptyList();
		}
		else {
			
			attributes = new ArrayList<>(pgAttrs.size());
	
			for (PgAttribute.Row pgAttr : pgAttrs) {
	
				Attribute attr = new Attribute();
				attr.number = pgAttr.number;
				attr.name = pgAttr.name;
				attr.type = registry.loadType(pgAttr.typeId);
				attr.nullable = pgAttr.nullable;
				attr.hasDefault = pgAttr.hasDefault;
				attr.typeModifiers = attr.type != null ? attr.type.getModifierParser().parse(pgAttr.typeModifier) : Collections.<String,Object>emptyMap();
				attr.autoIncrement = pgAttr.autoIncrement;
	
				attributes.add(attr);
			}
		
		}

	}

}
