package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.net.SocketAddress;

import com.impossibl.postgres.system.Context;

public interface ProtocolFactory {

	Protocol connect(SocketAddress address, Context context) throws IOException;
	
}
