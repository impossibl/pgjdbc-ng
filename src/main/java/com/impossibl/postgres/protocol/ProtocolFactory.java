package com.impossibl.postgres.protocol;

import java.io.IOException;
import java.net.SocketAddress;

import com.impossibl.postgres.system.BasicContext;

public interface ProtocolFactory {

	Protocol connect(SocketAddress address, BasicContext context) throws IOException;
	
}
