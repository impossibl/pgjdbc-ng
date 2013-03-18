package com.impossibl.postgres.types;

import java.util.Collection;

import com.impossibl.postgres.system.tables.PgType.Row;

/**
 * A database psuedo type.
 * 
 * @author kdubb
 *
 */
public class PsuedoType extends Type {
	
	PrimitiveType primitiveType;

	@Override
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
