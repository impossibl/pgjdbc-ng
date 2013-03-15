package com.impossibl.postgres.types;

import java.io.IOException;
import java.util.Collection;

import org.jboss.netty.buffer.ChannelBuffer;

import com.impossibl.postgres.protocol.ResultField.Format;
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
			int getInputSQLType();
			Class<?> getOutputType();
			Object decode(Type type,  ChannelBuffer buffer, Context context) throws IOException;
		}
		
		/**
		 * Encodes the given Java language as data the server expects.  
		 */
		public interface Encoder {
			Class<?> getInputType();
			int getOutputSQLType();
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
	private Type arrayType;
	private int relationId;
	private Codec binaryCodec;
	private Codec textCodec;
	private Modifiers.Parser modifierParser;	
	
	public Type() {
	}

	public Type(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, Codec binaryCodec, Codec textCodec) {
		super();
		this.id = id;
		this.name = name;
		this.length = length;
		this.alignment = alignment;
		this.category = category;
		this.delimeter = delimeter;
		this.arrayType = arrayType;
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
	
	public Type getArrayType() {
		return arrayType;
	}
	
	public void setArrayType(Type arrayType) {
		this.arrayType = arrayType;
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
		arrayType = registry.loadType(source.arrayTypeId);
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

	public Class<?> getInputType(Format format) {
		
		switch(format) {
		case Text:
			return textCodec.encoder.getInputType();
		case Binary:
			return binaryCodec.encoder.getInputType();
		}
		return null;
	}

	public Class<?> getOutputType(Format format) {
		
		switch(format) {
		case Text:
			return textCodec.decoder.getOutputType();
		case Binary:
			return binaryCodec.decoder.getOutputType();
		}
		return null;
	}

	public int getInputSQLType(Format format) {
		
		switch(format) {
		case Text:
			return textCodec.decoder.getInputSQLType();
		case Binary:
			return binaryCodec.decoder.getInputSQLType();
		}
		return 0;
	}

	public int getOutputSQLType(Format format) {
		
		switch(format) {
		case Text:
			return textCodec.encoder.getOutputSQLType();
		case Binary:
			return binaryCodec.encoder.getOutputSQLType();
		}
		return 0;
	}

}
