/*
 * Copyright 2017, OnGres.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */


package com.impossibl.postgres.protocol.sasl.scram.client;


import com.impossibl.postgres.jdbc.xa.Base64;
import com.impossibl.postgres.protocol.sasl.scram.ScramFunctions;
import com.impossibl.postgres.protocol.sasl.scram.ScramMechanism;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramInvalidServerSignatureException;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramParseException;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramServerErrorException;
import com.impossibl.postgres.protocol.sasl.scram.gssapi.Gs2CbindFlag;
import com.impossibl.postgres.protocol.sasl.scram.message.ClientFinalMessage;
import com.impossibl.postgres.protocol.sasl.scram.message.ClientFirstMessage;
import com.impossibl.postgres.protocol.sasl.scram.message.ServerFinalMessage;
import com.impossibl.postgres.protocol.sasl.scram.message.ServerFirstMessage;
import com.impossibl.postgres.protocol.sasl.scram.stringprep.StringPreparation;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotEmpty;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;


/**
 * A class that represents a SCRAM client. Use this class to perform a SCRAM negotiation with a SCRAM server.
 * This class performs an authentication execution for a given user, and has state related to it.
 * Thus, it cannot be shared across users or authentication executions.
 */
public class ScramSession {
  private final ScramMechanism scramMechanism;
  private final StringPreparation stringPreparation;
  private final String user;
  private final String nonce;
  private ClientFirstMessage clientFirstMessage;
  private String serverFirstMessageString;

  /**
   * Constructs a SCRAM client, to perform an authentication for a given user.
   * This class can be instantiated directly,
   * but it is recommended that a {@link ScramClient} is used instead.
   * @param scramMechanism The SCRAM mechanism that will be using this client
   * @param stringPreparation
   * @param user
   * @param nonce
   */
  public ScramSession(ScramMechanism scramMechanism, StringPreparation stringPreparation, String user, String nonce) {
    this.scramMechanism = checkNotNull(scramMechanism, "scramMechanism");
    this.stringPreparation = checkNotNull(stringPreparation, "stringPreparation");
    this.user = checkNotNull(user, "user");
    this.nonce = checkNotEmpty(nonce, "nonce");
  }

  private String setAndReturnClientFirstMessage(ClientFirstMessage clientFirstMessage) {
    this.clientFirstMessage = clientFirstMessage;

    return clientFirstMessage.toString();
  }

  /**
   * Returns the text representation of a SCRAM client-first-message, with the GSS-API header values indicated.
   * @param gs2CbindFlag The channel binding flag
   * @param cbindName The channel binding algorithm name, if channel binding is supported, or null
   * @param authzid The optional
   * @return The message
   */
  public String clientFirstMessage(Gs2CbindFlag gs2CbindFlag, String cbindName, String authzid) {
    return setAndReturnClientFirstMessage(new ClientFirstMessage(gs2CbindFlag, authzid, cbindName, user, nonce));
  }

  /**
   * Returns the text representation of a SCRAM client-first-message, with no channel binding nor authzid.
   * @return The message
   */
  public String clientFirstMessage() {
    return setAndReturnClientFirstMessage(new ClientFirstMessage(user, nonce));
  }

  /**
   * Process a received server-first-message.
   * Generate by calling {@link #receiveServerFirstMessage(String)}.
   */
  public class ServerFirstProcessor {
    private final ServerFirstMessage serverFirstMessage;

    private ServerFirstProcessor(String receivedServerFirstMessage) throws ScramParseException {
      serverFirstMessageString = receivedServerFirstMessage;
      serverFirstMessage = ServerFirstMessage.parseFrom(receivedServerFirstMessage, nonce);
    }

    public String getSalt() {
      return serverFirstMessage.getSalt();
    }

    public int getIteration() {
      return serverFirstMessage.getIteration();
    }

    /**
     * Generates a {@link ClientFinalProcessor}, that allows to generate the client-final-message and also
     * receive and parse the server-first-message. It is based on the user's password.
     * @param password The user's password
     * @return The handler
     * @throws IllegalArgumentException If the message is null or empty
     */
    public ClientFinalProcessor clientFinalProcessor(String password) throws IllegalArgumentException {
      return new ClientFinalProcessor(
          serverFirstMessage.getNonce(),
          checkNotEmpty(password, "password"),
          getSalt(),
          getIteration()
      );
    }

    /**
     * Generates a {@link ClientFinalProcessor}, that allows to generate the client-final-message and also
     * receive and parse the server-first-message. It is based on the clientKey and storedKey,
     * which, if available, provide an optimized path versus providing the original user's password.
     * @param clientKey The client key, as per the SCRAM algorithm.
     *                  It can be generated with:
     *                  {@link ScramFunctions#clientKey(ScramMechanism, StringPreparation, String, byte[], int)}
     * @param storedKey The stored key, as per the SCRAM algorithm.
     *                  It can be generated from the client key with:
     *                  {@link ScramFunctions#storedKey(ScramMechanism, byte[])}
     * @return The handler
     * @throws IllegalArgumentException If the message is null or empty
     */
    public ClientFinalProcessor clientFinalProcessor(byte[] clientKey, byte[] storedKey)
        throws IllegalArgumentException {
      return new ClientFinalProcessor(
          serverFirstMessage.getNonce(),
          checkNotNull(clientKey, "clientKey"),
          checkNotNull(storedKey, "storedKey")
      );
    }
  }

  /**
   * Processor that allows to generate the client-final-message,
   * as well as process the server-final-message and verify server's signature.
   * Generate the processor by calling either {@link ServerFirstProcessor#clientFinalProcessor(String)}
   * or {@link ServerFirstProcessor#clientFinalProcessor(byte[], byte[])}.
   */
  public class ClientFinalProcessor {
    private final String nonce;
    private final byte[] clientKey;
    private final byte[] storedKey;
    private final byte[] serverKey;
    private String authMessage;

    private ClientFinalProcessor(String nonce, byte[] clientKey, byte[] storedKey, byte[] serverKey) {
      assert null != clientKey : "clientKey";
      assert null != storedKey : "storedKey";
      assert null != serverKey : "serverKey";

      this.nonce = nonce;
      this.clientKey = clientKey;
      this.storedKey = storedKey;
      this.serverKey = serverKey;
    }

    private ClientFinalProcessor(String nonce, byte[] clientKey, byte[] serverKey) {
      this(nonce, clientKey, ScramFunctions.storedKey(scramMechanism, clientKey), serverKey);
    }

    private ClientFinalProcessor(String nonce, byte[] saltedPassword) {
      this(
          nonce,
          ScramFunctions.clientKey(scramMechanism, saltedPassword),
          ScramFunctions.serverKey(scramMechanism, saltedPassword)
      );
    }

    private ClientFinalProcessor(String nonce, String password, String salt, int iteration) {
      this(
          nonce,
          ScramFunctions.saltedPassword(
              scramMechanism, stringPreparation, password, Base64.decode(salt), iteration
          )
      );
    }

    private synchronized void generateAndCacheAuthMessage(byte[] cbindData) {
      if (null != authMessage) {
        return;
      }

      authMessage = clientFirstMessage.writeToWithoutGs2Header(new StringBuffer())
          .append(",")
          .append(serverFirstMessageString)
          .append(",")
          .append(ClientFinalMessage.writeToWithoutProof(clientFirstMessage.getGs2Header(), cbindData, nonce))
          .toString();
    }

    /**
     * Generates the SCRAM representation of the client-final-message, including the given channel-binding data.
     * @param cbindData The bytes of the channel-binding data
     * @return The message
     * @throws IllegalArgumentException If the channel binding data is null
     */
    public String clientFinalMessage(byte[] cbindData) throws IllegalArgumentException {
      if (null == authMessage) {
        generateAndCacheAuthMessage(cbindData);
      }

      ClientFinalMessage clientFinalMessage = new ClientFinalMessage(
          clientFirstMessage.getGs2Header(),
          cbindData,
          nonce,
          ScramFunctions.clientProof(
              clientKey,
              ScramFunctions.clientSignature(scramMechanism, storedKey, authMessage)
          )
      );

      return clientFinalMessage.toString();
    }

    /**
     * Generates the SCRAM representation of the client-final-message.
     * @return The message
     */
    public String clientFinalMessage() {
      return clientFinalMessage(null);
    }

    /**
     * Receive and process the server-final-message.
     * Server SCRAM signatures is verified.
     * @param serverFinalMessage The received server-final-message
     * @throws ScramParseException If the message is not a valid server-final-message
     * @throws ScramServerErrorException If the server-final-message contained an error
     * @throws IllegalArgumentException If the message is null or empty
     */
    public void receiveServerFinalMessage(String serverFinalMessage)
        throws ScramParseException, ScramServerErrorException, ScramInvalidServerSignatureException,
        IllegalArgumentException {
      checkNotEmpty(serverFinalMessage, "serverFinalMessage");

      ServerFinalMessage message = ServerFinalMessage.parseFrom(serverFinalMessage);
      if (message.isError()) {
        throw new ScramServerErrorException(message.getError());
      }
      if (!ScramFunctions.verifyServerSignature(
          scramMechanism, serverKey, authMessage, message.getVerifier()
      )) {
        throw new ScramInvalidServerSignatureException("Invalid server SCRAM signature");
      }
    }
  }

  public ScramMechanism getScramMechanism() {
    return scramMechanism;
  }

  /**
   * Constructs a handler for the server-first-message, from its String representation.
   * @param serverFirstMessage The message
   * @return The handler
   * @throws ScramParseException If the message is not a valid server-first-message
   * @throws IllegalArgumentException If the message is null or empty
   */
  public ServerFirstProcessor receiveServerFirstMessage(String serverFirstMessage)
      throws ScramParseException, IllegalArgumentException {
    return new ServerFirstProcessor(checkNotEmpty(serverFirstMessage, "serverFirstMessage"));
  }
}
