package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.protocol.ResponseHandler.Error;
import com.impossibl.postgres.protocol.ResponseHandler.Error.Severity;
import com.impossibl.postgres.utils.DataInputStream;

public class ErrorResponseMP implements MessageProcessor {

	@Override
	public void process(DataInputStream in, ResponseHandler handler) throws IOException {
		
		Error error = new Error();

		switch(in.readByte()) {
		case 'S':
			error.severity = Severity.valueOf(in.readCString());
			break;
			
		case 'C':
			error.code = in.readCString();
			break;
			
		case 'M':
			error.message = in.readCString();
			break;
			
		case 'D':
			error.detail = in.readCString();
			break;
			
		case 'H':
			error.hint = in.readCString();
			break;
			
		case 'P':
			error.position = Integer.parseInt(in.readCString());
			break;
			
		case 'F':
			error.file = in.readCString();
			break;
			
		case 'L':
			error.line = Integer.parseInt(in.readCString());
			break;
			
		case 'R':
			error.routine = in.readCString();
			break;
			
		default:
			//Read and ignore
			in.readCString();
			break;
		}
		
		handler.error(error);
	}

}
