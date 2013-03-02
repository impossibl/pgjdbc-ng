package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class DataRowMP implements MessageProcessor {

	@Override
	public void process(Protocol proto, DataInputStream in, Context context) throws IOException {

		Type tupleType = context.getResultType();
		
		tupleType.getBinaryIO().send.handle(tupleType, in, context);
		
	}

}
