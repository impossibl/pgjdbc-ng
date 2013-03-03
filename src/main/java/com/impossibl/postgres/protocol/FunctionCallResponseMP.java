package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class FunctionCallResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {
		
		Object value = null;
		
		int length = in.readInt();

		long start = in.getCount();
		
		if(length != -1) {
			
			Type resultType = handler.getResultsType();
			
			value = resultType.getBinaryIO().decoder.decode(resultType, in, handler.getContext());
		}
		
		if(length == (in.getCount() - start)) {
			throw new IOException("invalid result length");
		}
		
		handler.addResult(value);
	}

}
