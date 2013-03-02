package com.impossibl.postgres.protocol;

import java.io.IOException;

import com.impossibl.postgres.Context;
import com.impossibl.postgres.utils.DataInputStream;

public interface MessageProcessor {
	
	void process(Protocol protocol, DataInputStream in, Context context) throws IOException;

}
