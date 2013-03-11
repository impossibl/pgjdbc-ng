package com.impossibl.postgres.protocol.v30;

import java.io.IOException;

import com.impossibl.postgres.protocol.CloseCommand;
import com.impossibl.postgres.protocol.Error;
import com.impossibl.postgres.protocol.ServerObject;



public class CloseCommandImpl extends CommandImpl implements CloseCommand {
	
	ServerObject target;
	String targetName;
	boolean complete;

	private ProtocolListener listener = new BaseProtocolListener() {

		@Override
		public boolean isComplete() {
			return complete || error != null;
		}

		@Override
		public synchronized void closeComplete() {
			complete = true;
			notifyAll();
		}

		@Override
		public synchronized void error(Error error) {
			CloseCommandImpl.this.error = error;
			notifyAll();
		}

	};

	public CloseCommandImpl(ServerObject target, String targetName) {
		this.target = target;
		this.targetName = targetName;
	}

	@Override
	public ServerObject getTarget() {
		return target;
	}
	
	@Override
	public String getTargetName() {
		return targetName;
	}

	@Override
	public void execute(ProtocolImpl protocol) throws IOException {

		protocol.setListener(listener);
		
		protocol.sendClose(target, targetName);
		
		protocol.sendFlush();

		waitFor(listener);
		
	}

}
