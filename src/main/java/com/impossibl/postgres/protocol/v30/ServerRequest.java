package com.impossibl.postgres.protocol.v30;

import java.io.IOException;

public interface ServerRequest {

  ProtocolHandler createHandler();

  void execute(ProtocolChannel channel) throws IOException;

}
