package com.impossibl.postgres.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;



public class Protocol30 implements Protocol {

	private static final byte DESCRIBE_MSG_ID 			= 'D';
	private static final byte EXECUTE_MSG_ID 				= 'E';
	private static final byte FLUSH_MSG_ID 					= 'H';
	private static final byte FUNCTION_CALL_MSG_ID 	= 'F';
	private static final byte PARSE_MSG_ID 					= 'P';
	private static final byte BIND_MSG_ID 					= 'B';
	private static final byte PASSWORD_MSG_ID 			= 'p';
	private static final byte QUERY_MSG_ID 					= 'Q';	
	private static final byte TERMINATE_MSG_ID 			= 'X';
	private static final byte SYNC_MSG_ID 					= 'S';
	
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
	
	private Context context;
	private DataInputStream in;
	private DataOutputStream out;
	
	public Protocol30(Context context, DataInputStream in, DataOutputStream out) {
		this.context = context;
		this.in = in;
		this.out = out;
	}

	@Override
	public void dispatch(ResponseHandler handler) throws IOException {
		
		byte msgId = in.readByte();
		int dispatchId = msgId - '0';		
		MessageProcessor mp = DISPATCH_TABLE[dispatchId];
		
		long msgStart = in.getCount();

		int msgLength = in.readInt();
		
		mp.process(in, handler);

		if(msgLength != (in.getCount() - msgStart)) {
			throw new IOException("invalid message length");
		}
		
	}

	@Override
	public void startup(Map<String, Object> params) throws IOException {

		Message msg = new Message((byte)0);
		
		// Version
		msg.writeShort(3);
		msg.writeShort(0);

		// Name=Value pairs
		for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
			msg.writeCString(paramEntry.getKey());
			msg.writeCString(paramEntry.getValue().toString());
		}
		
		msg.writeByte(0);
		
		sendMessage(msg);
	}

	@Override
	public void authenticate(String response) throws IOException {
		
		Message msg = new Message(PASSWORD_MSG_ID);
		
		msg.writeCString(response);

		sendMessage(msg);
	}
	
	@Override
	public void query(String query) throws IOException {
		
		Message msg = new Message(QUERY_MSG_ID);
		
		msg.writeCString(query);
		
		sendMessage(msg);
	}
	
	@Override
	public void queryParse(String stmtName, String query, List<Type> paramTypes) throws IOException {
		
		Message msg = new Message(PARSE_MSG_ID);
		
		msg.writeCString(stmtName != null ? stmtName : "");
		msg.writeCString(query);

		msg.writeShort(paramTypes.size());
		for(Type paramType : paramTypes) {
			msg.writeInt(paramType.getId());
		}
				
		sendMessage(msg);
	}
	
	@Override
	public void queryBind(String portalName, String stmtName, List<Type> paramTypes, List<Object> paramValues) throws IOException {
		
		Message msg = new Message(BIND_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeCString(stmtName != null ? stmtName : "");

		loadParams(msg, paramTypes, paramValues);
		
		//Binary format for all results fields
		msg.writeShort(1);
		msg.writeShort(1);		

		sendMessage(msg);				
	}
	
	@Override
	public void queryExecute(String portalName, int maxRows) throws IOException {
		
		Message msg = new Message(EXECUTE_MSG_ID);
		
		msg.writeCString(portalName != null ? portalName : "");
		msg.writeInt(maxRows);
		
		sendMessage(msg);
	}

	@Override
	public void describe(char targetType, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(targetType);
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
	@Override
	public void close(char targetType, String targetName) throws IOException {
		
		Message msg = new Message(DESCRIBE_MSG_ID);
		
		msg.writeByte(targetType);
		msg.writeCString(targetName != null ? targetName : "");
		
		sendMessage(msg);
	}
	
	@Override
	public void functionCall(int functionId, List<Type> paramTypes, List<Object> paramValues) throws IOException {
		
		Message msg = new Message(FUNCTION_CALL_MSG_ID);
		
		msg.writeInt(functionId);
		
		loadParams(msg, paramTypes, paramValues);
		
		msg.writeShort(1);
		
		sendMessage(msg);
	}
	
	@Override
	public void flush() throws IOException {
		sendMessage(FLUSH_MSG_ID, 0);
	}

	@Override
	public void sync() throws IOException {
		sendMessage(SYNC_MSG_ID, 0);
	}

	@Override
	public void terminate() throws IOException {
		sendMessage(TERMINATE_MSG_ID, 0);
	}

	private void sendMessage(Message msg) throws IOException {
		
		ByteArrayOutputStream data = msg.getData();
		
		if(msg.getId() != 0)
			out.writeByte(msg.getId());
		
		out.writeInt(data.size()+4);
		out.write(data.toByteArray());
		
		out.flush();
	}

	private void sendMessage(byte msgId, int dataLength) throws IOException {
		out.writeByte(msgId);
		out.writeInt(dataLength+4);
	}

	private byte[] serialize(Type type, Object value) throws IOException {
		
		//OPTI: do this without allocating new byte streams
		
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream dataStream = new DataOutputStream(byteStream);
		
		type.getBinaryIO().encoder.encode(type, dataStream, value, context);
		
		return byteStream.toByteArray();
	}

	private void loadParams(DataOutputStream out, List<Type> paramTypes, List<Object> paramValues) throws IOException {
		
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
	
}
