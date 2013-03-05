package com.impossibl.postgres.types;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

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

		public static Category valueOf(byte id) {
			
			for(Category cat : values()) {
				if(cat.id == id)
					return cat;
			}
			
			return null;
		}
		
	}
	
	public static class BinaryIO {

		public interface Decoder {
			Object decode(Type type, DataInputStream stream, Context context) throws IOException;
		}
		
		public interface Encoder {
			void encode(Type tyoe, DataOutputStream stream, Object value, Context context) throws IOException;
		}

		public Decoder decoder;
		public Encoder encoder;
	}

	public static class TextIO {

		public interface Decoder {
			Object decode(Type type, Reader reader, Context context) throws IOException;
		}
		
		public interface Encoder {
			void encode(Type tyoe, Writer writer, Object value, Context context) throws IOException;
		}

		public Decoder decoder;
		public Encoder encoder;
	}

	private int id;
	private String name;
	private Short length;
	private Byte alignment;
	private Category category;
	private char delimeter;
	private Type arrayType;
	private BinaryIO binaryIO;
	private TextIO textIO;
	private int sqlType;
	
	
	public Type() {
	}

	public Type(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, BinaryIO binaryIO, TextIO textIO, int sqlType) {
		super();
		this.id = id;
		this.name = name;
		this.length = length;
		this.alignment = alignment;
		this.category = category;
		this.delimeter = delimeter;
		this.arrayType = arrayType;
		this.binaryIO = binaryIO;
		this.textIO = textIO;
		this.sqlType = sqlType;
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
	
	public BinaryIO getBinaryIO() {
		return binaryIO;
	}
	
	public void setBinaryIO(BinaryIO binaryIO) {
		this.binaryIO = binaryIO;
	}
	
	public TextIO getTextIO() {
		return textIO;
	}

	public void setTextIO(TextIO textIO) {
		this.textIO = textIO;
	}

	public int getSqlType() {
		return sqlType;
	}

	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}

	public void load(PgType.Row source, Collection<PgAttribute.Row> attrs) {
		
		id = source.oid;
		name = source.name;
		length = source.length != -1 ? source.length : null;
		alignment = getAlignment(source.alignment); 
		category = Category.valueOf(source.category);
		delimeter = (char)source.deliminator;
		arrayType = Registry.loadType(source.arrayTypeId);
		textIO = Registry.loadTextIO(source.inputId, source.outputId);
		binaryIO = Registry.loadBinaryIO(source.receiveId, source.sendId);
	}
	
	public static Byte getAlignment(Byte align) {
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
