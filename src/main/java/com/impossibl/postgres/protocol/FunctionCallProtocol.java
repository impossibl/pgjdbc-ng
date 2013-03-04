package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;

public class FunctionCallProtocol extends CommandProtocol {
	
	//Frontend messages
	private static final byte FUNCTION_CALL_MSG_ID 		= 'F';

	//Backend messages
	private static final byte FUNCTION_RESULT_MSG_ID 	= 'V';
	
	
	protected Type resultType;
	protected Object result;
	

	public FunctionCallProtocol(Context context) {
		super(context);
	}

	public void functionCall(int functionId, List<Type> paramTypes, List<Object> paramValues) throws IOException {
		
		Message msg = new Message(FUNCTION_CALL_MSG_ID);
		
		msg.writeInt(functionId);
		
		loadParams(msg, paramTypes, paramValues);
		
		msg.writeShort(1);
		
		sendMessage(msg);
	}
		
	@Override
	public boolean dispatch(DataInputStream in, byte msgId) throws IOException {
		
		if(super.dispatch(in, msgId))
			return true;
		
		switch(msgId) {
		case FUNCTION_RESULT_MSG_ID:
			receiveFunctionResult(in);
			return true;
			
		}
		
		return false;
	}


	protected Type getResultType() throws IOException {
		return resultType;
	}
	
	protected void functionResult(Object value) throws IOException {
		result = value;
	}

	protected void receiveFunctionResult(DataInputStream in) throws IOException {

		Object value = null;
		
		int length = in.readInt();

		long start = in.getCount();
		
		if(length != -1) {
			
			Type resultType = getResultType();
			
			value = resultType.getBinaryIO().decoder.decode(resultType, in, context);
		}
		
		if(length == (in.getCount() - start)) {
			throw new IOException("invalid result length");
		}
		
		functionResult(value);
	}
	
}
