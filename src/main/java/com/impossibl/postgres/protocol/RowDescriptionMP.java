package com.impossibl.postgres.protocol;

import static java.util.Arrays.asList;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Tuple;
import com.impossibl.postgres.utils.DataInputStream;

public class RowDescriptionMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {
		
		short fieldCount = in.readShort();

		Field[] fields = new Field[fieldCount];

		for (int c = 0; c < fieldCount; ++c) {

			Field field = new Field();
			field.name = in.readCString();
			field.relationId = in.readInt();
			field.attributeIndex = in.readShort();
			field.typeId = in.readInt();
			field.typeLength = in.readShort();
			field.typeModId = in.readInt();
			field.formatCode = in.readShort();

			fields[c] = field;
		}
		
		Tuple tupleType = context.createTupleType(asList(fields));
		
		context.setResultType(tupleType);
	}

}
