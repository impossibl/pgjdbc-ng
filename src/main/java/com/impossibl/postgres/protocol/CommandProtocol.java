package com.impossibl.postgres.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public class CommandProtocol extends Protocol {

	//Frontend messages
	private static final byte FLUSH_MSG_ID 					= 'H';
	private static final byte TERMINATE_MSG_ID 			= 'X';
	private static final byte SYNC_MSG_ID 					= 'S';

	//Backend messages
	private static final byte NOTIFICATION_MSG_ID = 'A';
	private static final byte COMMAND_COMPLETE_MSG_ID = 'C';


	public CommandProtocol(Context context) {
		super(context);
	}

	public void flush() throws IOException {
		sendMessage(FLUSH_MSG_ID, 0);
	}

	public void sync() throws IOException {
		sendMessage(SYNC_MSG_ID, 0);
	}

	public void terminate() throws IOException {
		sendMessage(TERMINATE_MSG_ID, 0);
	}

	protected void loadParams(DataOutputStream out, List<Type> paramTypes, List<Object> paramValues) throws IOException {
		
		//Binary format for all parameters
		out.writeShort(1);
		out.writeShort(1);

		//Values for each parameter
		out.writeShort(paramTypes.size());
		for(int c=0; c < paramTypes.size(); ++c) {
			
			Type paramType = paramTypes.get(c);
			Object paramValue = paramValues.get(c);
			
			if(paramValue == null) {
				out.writeInt(-1);
			}
			else {
				byte[] paramData = serialize(paramType, paramValue);
				out.writeInt(paramData.length);
				out.write(paramData);
			}
		}
		
	}
	
	protected byte[] serialize(Type type, Object value) throws IOException {
		
		//OPTI: do this without allocating new byte streams
		
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream dataStream = new DataOutputStream(byteStream);
		
		type.getBinaryIO().encoder.encode(type, dataStream, value, context);
		
		return byteStream.toByteArray();
	}


	@Override
	public boolean dispatch(DataInputStream in, byte msgId) throws IOException {
		
		if(super.dispatch(in, msgId))
			return true;
		
		switch(msgId) {
		case NOTIFICATION_MSG_ID:
			receiveNotification(in);
			return true;
			
		case COMMAND_COMPLETE_MSG_ID:
			receiveCommandComplete(in);
			return true;			
		}
		
		return false;
	}

	
	protected void commandComplete(String commandTag) {
	}
	
	private void receiveCommandComplete(DataInputStream in) throws IOException {
		
		String commandTag = in.readCString();
		
		commandComplete(commandTag);
	}


	protected void notification(int processId, String channelName, String payload) throws IOException {
		context.reportNotification(processId, channelName, payload);
	}

	protected void receiveNotification(DataInputStream in) throws IOException {
		
		int processId = in.readInt();
		String channelName = in.readCString();
		String payload = in.readCString();
		
		notification(processId, channelName, payload);
	}

}
