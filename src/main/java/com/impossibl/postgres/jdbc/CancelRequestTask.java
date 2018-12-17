package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.system.Context;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class CancelRequestTask extends ExecutionTimerTask {

  private SocketAddress serverAddress;
  private Context.KeyData keyData;

  CancelRequestTask(SocketAddress serverAddress, Context.KeyData keyData) {
    this.serverAddress = serverAddress;
    this.keyData = keyData;
  }

  @Override
  public void go() {
    sendCancelRequest();
  }

  private void sendCancelRequest() {

    try {

      if (serverAddress instanceof InetSocketAddress) {

        InetSocketAddress target = (InetSocketAddress) serverAddress;

        try (Socket abortSocket = new Socket(target.getAddress(), target.getPort())) {
          writeCancelRequest(new DataOutputStream(abortSocket.getOutputStream()));
        }

      }
      else {

        // Implement non IP socket when server connection supports it

      }
    }
    catch (IOException ignored) {
      // All exceptions during a cancellation attempt are ignored...
    }

  }

  private void writeCancelRequest(DataOutputStream os) throws IOException {

    os.writeInt(16);
    os.writeInt(80877102);
    os.writeInt(keyData.getProcessId());
    os.writeInt(keyData.getSecretKey());
  }

}
