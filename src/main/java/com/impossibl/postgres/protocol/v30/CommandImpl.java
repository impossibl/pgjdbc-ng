package com.impossibl.postgres.protocol.v30;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.impossibl.postgres.protocol.Notice;



public abstract class CommandImpl {

	protected Notice error;
	protected List<Notice> notices;
	
	public Notice getError() {
		return error;		
	}

	public void setError(Notice error) {
		this.error = error;
	}
	
	public List<Notice> getWarnings() {
		
		if(notices == null)
			return emptyList();
		
		List<Notice> warnings = new ArrayList<>();
		
		for(Notice notice : notices) {
			
			if(notice.isWarning())
				warnings.add(notice);
		}
		
		return warnings;
	}

	public void waitFor(ProtocolListener listener) {

		synchronized(listener) {

			while(listener.isComplete() == false) {

				try {
					listener.wait();
				}
				catch(InterruptedException e) {
					// Ignore
				}

			}

		}

	}

	public abstract void execute(ProtocolImpl protocol) throws IOException;

}
