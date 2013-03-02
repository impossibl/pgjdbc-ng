package com.impossibl.postgres.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Protocol30 implements Protocol {

	private static final char DESCRIBE_MSG_ID 			= 'D';
	private static final char EXECUTE_MSG_ID 				= 'E';
	private static final char FLUSH_MSG_ID 					= 'H';
	private static final char FUNCTION_CALL_MSG_ID 	= 'F';
	private static final char PARSE_MSG_ID 					= 'P';
	private static final char PASSWORD_MSG_ID 			= 'p';
	private static final char QUERY_MSG_ID 					= 'Q';	
	private static final char TERMINATE_MSG_ID 			= 'X';
	private static final char SYNC_MSG_ID 					= 'S';
	
	private static final MessageProcessor[] DISPATCH_TABLE = {
		null, //0
		new ParseCompleteMP(), 				//1
		new BindCompleteMP(), 				//2
		new CloseCompleteMP(), 				//3
		null, //4
		null, //5
		null, //6
		null, //7
		null, //8
		null, //9
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		new NotificationResponseMP(),	//A
		null, //B
		new CommandCompleteMP(), 			//C
		new DataRowMP(),							//D
		new ErrorResponseMP(), 				//E
		null, //F
		null, //G
		null, //H
		new EmptyQueryResponseMP(), 	//I
		null, //J
		new BackendKeyDataMP(),				//K
		null, //L
		null, //M
		new NoticeResponseMP(), 			//N
		null, //O
		null, //P
		null, //Q
		new AuthenticationMP(),				//R
		new ParameterStatusMP(), 			//S
		new RowDescriptionMP(), 			//T
		null, //U
		new FunctionCallResponseMP(), //V
		null, //W
		null, //X
		null, //Y
		new ReadyForQueryMP(), 				//Z
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		null, //unused
		null, //a
		null, //b
		null, //c
		null, //d
		null, //e
		null, //f
		null, //g
		null, //h
		null, //i
		null, //j
		null, //k
		null, //l
		null, //m
		new NoDataMP(), 							//n
		null, //o
		null, //p
		null, //q
		null, //r
		new PortalSuspendedMP(), 			//s
		new ParameterDescriptionMP(), //t
		null, //u
		null, //v
		null, //w
		null, //x
		null, //y
		null, //z
	};
	
	
	private ByteArrayOutputStream messageOut = new ByteArrayOutputStream();
	private DataOutputStream messageDataOut = new DataOutputStream(messageOut);
	
	
	@Override
	public void dispatch(DataInputStream in, Context context) throws IOException {
		
		byte msgId = in.readByte();
		int dispatchId = msgId - 65;		
		MessageProcessor mp = DISPATCH_TABLE[dispatchId];
		
		long msgStart = in.getCount();
		
		int msgLength = in.readInt();

		mp.process(this, in, context);
		
		if(msgLength != (in.getCount() - msgStart)) {
			throw new IOException("invalid message length");
		}
	}
	
	void startup(short protoVersionMajor, short protoVersionMinor, Map<String, Object> params) throws IOException {

		DataOutputStream out = beginMessage((char)0);
		
		// Version
		out.writeShort(protoVersionMajor);
		out.writeShort(protoVersionMinor);

		// Name=Value pairs
		for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
			out.writeCString(paramEntry.getKey());
			out.writeCString(paramEntry.getValue().toString());
		}
		
		endMessage((char) 0);
	}

	void sync() throws IOException {
		beginMessage();
		endMessage(SYNC_MSG_ID);
	}

	void terminate() throws IOException {
		beginMessage();
		endMessage(TERMINATE_MSG_ID);
	}

	@Override
	public void authenticate(String response) throws IOException {
		
		DataOutputStream out = beginMessage();
		
		out.writeCString(response);
		
		endMessage(PASSWORD_MSG_ID);
	}

	private DataOutputStream beginMessage(byte code) throws IOException {
		messageOut.reset();
		messageDataOut.writeByte(code);
		messageDataOut.writeInt(-1);
		return messageDataOut;
	}

	private void endMessage(DataOutputStream out) {
		messageOut.reset(1);
	}

}
