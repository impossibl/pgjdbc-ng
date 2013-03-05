package com.impossibl.postgres.protocol;

import static com.impossibl.postgres.protocol.TransactionStatus.Active;
import static com.impossibl.postgres.protocol.TransactionStatus.Failed;
import static com.impossibl.postgres.protocol.TransactionStatus.Idle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;
import com.impossibl.postgres.utils.DataOutputStream;

public class Protocol {
	
	private static Logger logger = Logger.getLogger(Protocol.class.getName());
	
	//Backend messages
	private static final byte ERROR_MSG_ID = 'E';
	private static final byte NOTICE_MSG_ID = 'N';
	private static final byte PARAMETER_STATUS_MSG_ID = 'S';
	private static final byte READY_FOR_QUERY_MSG_ID = 'Z';
	
	
	Context context;
	Error error;
	
	
	public Protocol(Context context) {
		this.context = context;
	}
	
	public Error getError() {
		return error;
	}
	
	public void run() throws IOException {
		
		while(!isRunComplete())
			receive();
	}
	
	public boolean isRunComplete() {
		return error != null;
	}
	
	protected void sendMessage(Message msg) throws IOException {
		
		DataOutputStream out = context.getOutputStream();

		ByteArrayOutputStream data = msg.getData();
		
		if(msg.getId() != 0)
			out.writeByte(msg.getId());
		
		out.writeInt(data.size()+4);
		out.write(data.toByteArray());
		
		out.flush();
	}

	protected void sendMessage(byte msgId, int dataLength) throws IOException {

		DataOutputStream out = context.getOutputStream();
		
		out.writeByte(msgId);
		
		out.writeInt(dataLength+4);
	}

	protected void error(Error error) throws IOException {
		context.reportError(error);
		this.error = error;
	}

	protected void notice(byte type, String data) throws IOException {
		context.reportNotice(type, data);
	}
	
	protected void parameterStatus(String name, String value) throws IOException {
		context.updateSystemParameter(name, value);
	}

	protected void readyForQuery(TransactionStatus txStatus) throws IOException {
	}

	
	/*
	 * 
	 * Message dispatching & parsing
	 * 
	 */

	
	protected boolean receive() throws IOException {
		
		DataInputStream in = context.getInputStream();
		
		if(in.available() < 1)
			return false;
		
		byte msgId = in.readByte();
		
		long msgStart = in.getCount();
		
		long msgLength = in.readInt();

		try {
			
			dispatch(in, msgId);
			
			return true;
		}
		finally {
			//Swallow leftover bytes in the event
			//the message dispatch failed
			long leftover = msgLength - (in.getCount() - msgStart);
			if(leftover > 0) {
				in.skip(leftover);
			}
		}
		
	}
	
	protected boolean dispatch(DataInputStream in, byte msgId) throws IOException {
		
		switch(msgId) {
		case ERROR_MSG_ID:
			receiveError(in);
			return true;
			
		case NOTICE_MSG_ID:
			receiveNotice(in);
			return true;
			
		case PARAMETER_STATUS_MSG_ID:
			receiveParameterStatus(in);
			return true;
			
		case READY_FOR_QUERY_MSG_ID:
			receiveReadyForQuery(in);
			return true;
		}
		
		return false;
	}

	
	private void receiveError(DataInputStream in) throws IOException {
		
		Error error = new Error();
	
		byte msgId;
		
		while((msgId = in.readByte()) != 0) {
			
			switch(msgId) {
			case 'S':
				error.severity = Error.Severity.valueOf(in.readCString());
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
			
		}
		
		logger.finest("ERROR: " + error.message);
		
		error(error);
	}

	
	private void receiveNotice(DataInputStream in) throws IOException {
		
		byte type;
		
		while((type = in.readByte()) != 0) {
			
			String value = in.readCString();
			
			notice(type, value);

			logger.finest("NOTICE: " + type + " - " + value);
			
		}
		
	}
	
	
	private void receiveParameterStatus(DataInputStream in) throws IOException {
		
		String name = in.readCString();
		String value = in.readCString();

		parameterStatus(name, value);
	}
	

	private void receiveReadyForQuery(DataInputStream in) throws IOException {
		
		TransactionStatus txStatus;
		
		switch(in.readByte()) {
		case 'T':
			txStatus = Active;
			break;
		case 'F':
			txStatus = Failed;
			break;
		case 'I':
			txStatus = Idle;
			break;
		default:
			throw new IllegalStateException("invalid transaction status");
		}
		
		logger.finest("READY: " + txStatus);
		
		readyForQuery(txStatus);
	}

}
