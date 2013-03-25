package com.impossibl.postgres.protocol.v30;

import java.io.IOException;

import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;



public class CloseCommandImpl extends CommandImpl implements CloseCommand {
	
	ServerObjectType objectType;
	String objectName;
	boolean complete;

	private ProtocolListener listener = new BaseProtocolListener() {

		@Override
		public boolean isComplete() {
			return complete || error != null;
		}

		@Override
		public void closeComplete() {
			complete = true;
		}

		@Override
		public void error(Notice error) {
			CloseCommandImpl.this.error = error;
		}

		@Override
		public void notice(Notice notice) {
			addNotice(notice);
		}

		@Override
		public synchronized void ready(TransactionStatus txStatus) {
			notifyAll();
		}

	};

	public CloseCommandImpl(ServerObjectType objectType, String objectName) {
		this.objectType = objectType;
		this.objectName = objectName;
	}

	@Override
	public ServerObjectType getObjectType() {
		return objectType;
	}
	
	@Override
	public String getObjectName() {
		return objectName;
	}

	@Override
	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.setListener(listener);
		
		protocol.sendClose(objectType, objectName);
		
		protocol.sendSync();

		waitFor(listener);
		
	}

}
