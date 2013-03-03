package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class FunctionCallResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, Context context) throws IOException {
		
		Object value = null;
		
		int length = in.readInt();

		long start = in.getCount();
		
		if(length != -1) {
			
			Type resultType = context.getResultType();
			
			value = resultType.getBinaryIO().decoder.decode(resultType, in, context);
		}
		
		if(length == (in.getCount() - start)) {
			throw new IOException("invalid result length");
		}
		
		context.setResultData(value);
	}

}
