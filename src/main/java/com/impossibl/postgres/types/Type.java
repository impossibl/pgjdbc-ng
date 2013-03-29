package com.impossibl.postgres.types;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;

/**
 * 
 * Represents a single type in the databases known types table. Type is the
 * base of a hierarchy that mirrors the kinds of types they represent.
 * 
 *  
 * NOTE: A Type, or one of its derived types, represents a single entry in
 * the "pg_type" table.
 *  
 * @author kdubb
 *
 */
public abstract class Type {
	
	public enum Category {
		Array						('A'),
		Boolean					('B'),
		Composite				('C'),
		DateTime				('D'),
		Enumeration			('E'),
		Geometry				('G'),
		NetworkAddress	('I'),
		Numeric					('N'),
		Psuedo					('P'),
		Range						('R'),
		String					('S'),
		Timespan				('T'),
		User						('U'),
		BitString				('V'),
		Unknown					('X')
		;
		
		private char id;
		
		Category(char id) {
			this.id = id;
		}
		
		public char getId() {
			return id;
		}

		/**
		 * Lookup Category by its associated "id".
		 * 
		 * @param id
		 * @return Associated category or null if none
		 */
		public static Category findValue(String id) {
			
			if(id == null || id.isEmpty())
				return null;
			
			for(Category cat : values()) {
				if(cat.id == id.charAt(0))
					return cat;
			}
			
			return null;
		}
		
	}
	
	/**
	 * A pair of related interface methods to encode/decode a type in a
	 * specific format.  The are mapped to their equivalent procedures
	 * in the database.
	 */
	public static class Codec {

		/**
		 *	Decodes the given data into a Java language object
		 */
		public interface Decoder {
			PrimitiveType getInputPrimitiveType();
			Class<?> getOutputType();
			Object decode(Type type,  ChannelBuffer buffer, Context context) throws IOException;
		}
		
		/**
		 * Encodes the given Java language as data the server expects.  
		 */
		public interface Encoder {
			Class<?> getInputType();
			PrimitiveType getOutputPrimitiveType();
			void encode(Type tyoe, ChannelBuffer buffer, Object value, Context context) throws IOException;
		}

		public Decoder decoder;
		public Encoder encoder;
	}
	
	private int id;
	private String name;
	private String namespace;
	private Short length;
	private Byte alignment;
	private Category category;
	private Character delimeter;
	private int arrayTypeId;
	private int relationId;
	private Codec binaryCodec;
	private Codec textCodec;
	private Modifiers.Parser modifierParser;	
	
	public Type() {
	}

	public Type(int id, String name, Short length, Byte alignment, Category category, char delimeter, int arrayTypeId, Codec binaryCodec, Codec textCodec) {
		super();
		this.id = id;
		this.name = name;
		this.length = length;
		this.alignment = alignment;
		this.category = category;
		this.delimeter = delimeter;
		this.arrayTypeId = arrayTypeId;
		this.binaryCodec = binaryCodec;
		this.textCodec = textCodec;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Short getLength() {
		return length;
	}
	
	public void setLength(Short length) {
		this.length = length;
	}
	
	public Byte getAlignment() {
		return alignment;
	}
	
	public void setAlignment(Byte alignment) {
		this.alignment = alignment;
	}
	
	public Category getCategory() {
		return category;
	}
	
	public void setCategory(Category category) {
		this.category = category;
	}
	
	public char getDelimeter() {
		return delimeter;
	}
	
	public void setDelimeter(char delimeter) {
		this.delimeter = delimeter;
	}
	
	public int getArrayTypeId() {
		return arrayTypeId;
	}
	
	public void setArrayTypeId(int arrayTypeId) {
		this.arrayTypeId = arrayTypeId;
	}
	
	public Codec getBinaryCodec() {
		return binaryCodec;
	}
	
	public void setBinaryCodec(Codec binaryCodec) {
		this.binaryCodec = binaryCodec;
	}
	
	public Codec getTextCodec() {
		return textCodec;
	}

	public void setTextCodec(Codec textCodec) {
		this.textCodec = textCodec;
	}

	public Modifiers.Parser getModifierParser() {
		return modifierParser;
	}

	public void setModifierParser(Modifiers.Parser modifierParser) {
		this.modifierParser = modifierParser;
	}

	public int getRelationId() {
		return relationId;
	}
	
	public void setRelationId(int relationId) {
		this.relationId = relationId;
	}

	/**
	 * Strips all "wrapping" type (e.g. arrays, domains) and returns
	 * the base type
	 * 
	 * @return Base type after all unwrapping
	 */
	public Type unwrap() {
		return this;
	}
	
	public PrimitiveType getPrimitiveType() {
		Codec codec;
		
		codec = getBinaryCodec();
		if(codec != null && codec.decoder.getInputPrimitiveType() != null) {
			return codec.decoder.getInputPrimitiveType();
		}
		
		codec = getTextCodec();
		if(codec != null && codec.decoder.getInputPrimitiveType() != null) {
			return codec.decoder.getInputPrimitiveType();
		}
		
		return PrimitiveType.Unknown;
	}
	
	public Class<?> getJavaType(Map<String, Class<?>> customizations) {
		Codec codec;
		
		codec = getBinaryCodec();
		if(codec != null) {
			return codec.decoder.getOutputType();
		}
		
		codec = getTextCodec();
		if(codec != null) {
			return codec.decoder.getOutputType();
		}
		
		return null;
	}
	
	/**
	 * Load this type from a "pg_type" table entry and, if available, a
	 * collection of "pg_attribute" table entries.
	 * 
	 * @param source The "pg_type" table entry
	 * @param attrs Associated "pg_attribute" table entries, if available.
	 * @param registry The registry that is loading the type.
	 */
	public void load(PgType.Row source, Collection<PgAttribute.Row> attrs, Registry registry) {
		
		id = source.oid;
		name = source.name;
		namespace = source.namespace;
		length = source.length != -1 ? source.length : null;
		alignment = getAlignment(source.alignment != null ? source.alignment.charAt(0) : null); 
		category = Category.findValue(source.category);
		delimeter = source.deliminator != null ? source.deliminator.charAt(0) : null;
		arrayTypeId = source.arrayTypeId;
		relationId = source.relationId;
		textCodec = registry.loadCodec(source.inputId, source.outputId);
		binaryCodec = registry.loadCodec(source.receiveId, source.sendId);
		modifierParser = registry.loadModifierParser(source.modInId, source.modOutId);		
	}
	
	/**
	 * Translates a protocol alignment id into a specific number of bytes.
	 * 
	 * @param align Alignment ID
	 * @return # of bytes to align on
	 */
	public static Byte getAlignment(Character align) {
		
		if(align == null)
			return null;
		
		switch(align) {
		case 'c':
			return 1;
		case 's':
			return 2;
		case 'i':
			return 4;
		case 'd':
			return 8;
		}

		throw new IllegalStateException("invalid alignment character");
	}

	@Override
	public String toString() {
		return name + '(' + id + ')';
	}

}
