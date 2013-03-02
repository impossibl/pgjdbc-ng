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

		public interface SendHandler {
			Object handle(Type type, DataInputStream stream, Context context) throws IOException;
		}
		
		public interface ReceiveHandler {
			void handle(Type tyoe, DataOutputStream stream, Object value, Context context) throws IOException;
		}

		public SendHandler send;
		public ReceiveHandler recv;
	}

	public static class TextIO {

		public interface OutputHandler {
			Object handle(Type type, Reader reader, Context context);
		}
		
		public interface InputHandler {
			void handle(Type tyoe, Writer writer, Object value, Context context) throws IOException;
		}

		public OutputHandler output;
		public InputHandler input;
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
		length = source.length;
		alignment = source.alignment; 
		category = Category.valueOf(source.category);
		delimeter = (char)source.deliminator;
		arrayType = Registry.loadType(source.arrayTypeId);
		textIO = Registry.loadTextIO(source.inputId, source.outputId);
		binaryIO = Registry.loadBinaryIO(source.receiveId, source.sendId);
	}
	
	@Override
	public String toString() {
		return name + '(' + id + ')';
	}

}
