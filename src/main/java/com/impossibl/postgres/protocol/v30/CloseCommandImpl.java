package com.impossibl.postgres.protocol.v30;

import java.io.IOException;

import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.Notice;
import com.impossibl.postgres.protocol.ServerObjectType;
import com.impossibl.postgres.protocol.TransactionStatus;



public class CloseCommandImpl extends CommandImpl implements CloseCommand {
	
	ServerObjectType objectType;
	long objectId;
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

	public CloseCommandImpl(ServerObjectType objectType, long objectId) {
		this.objectType = objectType;
		this.objectId = objectId;
	}

	@Override
	public ServerObjectType getObjectType() {
		return objectType;
	}
	
	@Override
	public long getObjectId() {
		return objectId;
	}

	@Override
	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.setListener(listener);
		
		protocol.sendClose(objectType, objectId);
		
		protocol.sendSync();

		waitFor(listener);
		
	}

}
