package com.impossibl.postgres.protocol.v30;

import java.io.IOException;

import com.impossibl.postgres.protocol.Error;

public abstract class CommandImpl {
	
	protected Error error;
	
	public Error getError() {
		return error;
	}

	public void setError(Error error) {
		this.error = error;
	}
	
	public void waitFor(ProtocolHandler handler) {

		synchronized(handler) {
			
			while(handler.isComplete() == false) {
				
				try {
					handler.wait();
				}
				catch(InterruptedException e) {
					//Ignore
				}
				
			}

		}
		
	}
	
	public abstract void execute(ProtocolImpl protocol) throws IOException;

}
