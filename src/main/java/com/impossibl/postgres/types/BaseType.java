package com.impossibl.postgres.types;

import static com.impossibl.postgres.system.procs.Procs.loadNamedBinaryCodec;
import static com.impossibl.postgres.system.procs.Procs.loadNamedTextCodec;

import java.util.Collection;

import com.impossibl.postgres.system.tables.PgType.Row;

/**
 * A database primitive type
 * 
 * @author kdubb
 *
 */
public class BaseType extends Type {
	
	PrimitiveType primitiveType; 

	public BaseType() {
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, Codec binaryCodec, Codec textCodec) {
		super(id, name, length, alignment, category, delimeter, arrayType, binaryCodec, textCodec);
		deducePrimitiveType(binaryCodec, textCodec);
	}

	public BaseType(int id, String name, Short length, Byte alignment, Category category, char delimeter, Type arrayType, String procName) {
		super(id, name, length, alignment, category, delimeter, arrayType, loadNamedBinaryCodec(procName, null), loadNamedTextCodec(procName, null));
		deducePrimitiveType(getBinaryCodec(), getTextCodec());
	}
	
	public PrimitiveType getPrimitiveType() {
		return primitiveType;
	}
	
	@Override
	public void load(Row source, Collection<com.impossibl.postgres.system.tables.PgAttribute.Row> attrs, Registry registry) {
		super.load(source, attrs, registry);
		deducePrimitiveType(getBinaryCodec(), getTextCodec());
	}

	private void deducePrimitiveType(Codec binaryCodec, Codec textCodec) {
		
		if(binaryCodec.encoder.getOutputPrimitiveType() != binaryCodec.decoder.getInputPrimitiveType() &&
				binaryCodec.encoder.getOutputPrimitiveType() != textCodec.decoder.getInputPrimitiveType() &&
				binaryCodec.encoder.getOutputPrimitiveType() != textCodec.encoder.getOutputPrimitiveType()) {
			//TODO log warning
		}
		
		primitiveType = binaryCodec.encoder.getOutputPrimitiveType();
		if(primitiveType != null) {
			return;
		}
		primitiveType = binaryCodec.decoder.getInputPrimitiveType();
		if(primitiveType != null) {
			return;
		}
		primitiveType = textCodec.encoder.getOutputPrimitiveType();
		if(primitiveType != null) {
			return;
		}
		primitiveType = textCodec.decoder.getInputPrimitiveType();
		if(primitiveType != null) {
			return;
		}
		
	}
	
}
