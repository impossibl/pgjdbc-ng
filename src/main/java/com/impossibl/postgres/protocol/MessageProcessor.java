package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.utils.DataInputStream;

public interface MessageProcessor {
	
	void process(DataInputStream in, ResponseHandler handler) throws IOException;

}
