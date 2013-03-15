package com.impossibl.postgres.types;

import static com.impossibl.postgres.system.procs.Procs.loadNamedBinaryCodec;
import static com.impossibl.postgres.system.procs.Procs.loadNamedTextCodec;

import java.util.Arrays;
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

	public CompositeType(int id, String name, Type arrayType, String procName) {
		super(id, name, null, null, Category.Composite, ',', arrayType, loadNamedBinaryCodec(procName, null), loadNamedTextCodec(procName, null));
	}

	public CompositeType(int id, String name, Type arrayType) {
		this(id, name, arrayType, "record_");
	}

	public CompositeType() {
	}

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
	public void load(PgType.Row pgType, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> pgAttrs, Registry registry) {

		super.load(pgType, pgAttrs, registry);

		Attribute[] attributes = new Attribute[pgAttrs.size()];

		int lastFreeIdx = attributes.length - 1;

		for (PgAttribute.Row pgAttr : pgAttrs) {

			Attribute attr = new Attribute();
			attr.name = pgAttr.name;
			attr.type = registry.loadType(pgAttr.typeId);
			attr.nullable = pgAttr.nullable;
			attr.hasDefault = pgAttr.hasDefault;
			attr.typeModifiers = attr.type != null ? attr.type.getModifierParser().parse(pgAttr.typeModifier) : Collections.<String,Object>emptyMap();

			int idx;

			// System columns have arbitrary
			// negative numbers. We assign
			// them a free slot from the
			// end of the list
			if (pgAttr.number < 1) {
				idx = lastFreeIdx--;
			}
			else {
				idx = pgAttr.number - 1;
			}

			attributes[idx] = attr;
		}

		this.attributes = Arrays.asList(attributes);
	}

}
