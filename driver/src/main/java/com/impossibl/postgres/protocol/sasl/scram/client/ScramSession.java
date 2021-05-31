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
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramException;
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

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * A class that represents a SCRAM client. Use this class to perform a SCRAM negotiation with a SCRAM server.
 * This class performs an authentication execution for a given user, and has state related to it.
 * Thus, it cannot be shared across users or authentication executions.
 */
public class ScramSession {
  private final ScramMechanism scramMechanism;
  private final String channelBindMethod;
  private final boolean serverSupportsChannelBinding;
  private final StringPreparation stringPreparation;
  private final String user;
  private final String nonce;
  private ClientFirstMessage clientFirstMessage;
  private ClientFinalProcessor clientFinalProcessor;
  private String serverFirstMessageString;

  /**
   * Constructs a SCRAM session, to perform a specific authentication flow. Use {@link ScramSessionFactory#start(String)} to
   * instantiate a ScramSession.
   *
   * @param scramMechanism The SCRAM mechanism that will be using this client
   * @param channelBindMethod Name of the selected channel-bind method
   * @param serverSupportsChannelBinding Whether the server reported supporting channel binding
   * @param stringPreparation String preparation implementation
   * @param user The user's name
   * @param nonce Randomly generated nonce
   */
  ScramSession(ScramMechanism scramMechanism, String channelBindMethod, boolean serverSupportsChannelBinding,
               StringPreparation stringPreparation, String user, String nonce) {
    this.scramMechanism = checkNotNull(scramMechanism, "scramMechanism");
    this.channelBindMethod = channelBindMethod;
    this.serverSupportsChannelBinding = serverSupportsChannelBinding;
    this.stringPreparation = checkNotNull(stringPreparation, "stringPreparation");
    this.user = checkNotNull(user, "user");
    this.nonce = checkNotEmpty(nonce, "nonce");
  }

  /**
   * Name of the session's selected SCRAM mechanism.
   */
  public String getScramMechanismName() {
    return scramMechanism.getName();
  }

  /**
   * Returns the text representation of a SCRAM client-first-message
   * @param authzid Optional authzid (may be null)
   * @return The message
   */
  public byte[] clientFirstMessage(String authzid) {

    Gs2CbindFlag gs2CbindFlag;
    if (channelBindMethod != null) {
      if (scramMechanism.requiresChannelBinding()) {
        gs2CbindFlag = Gs2CbindFlag.ENABLED;
      }
      else {
        gs2CbindFlag = serverSupportsChannelBinding ? Gs2CbindFlag.DISABLED : Gs2CbindFlag.NO_SERVER_SUPPORT;
      }
    }
    else {
      gs2CbindFlag = Gs2CbindFlag.DISABLED;
    }

    String channelBindMethod = gs2CbindFlag == Gs2CbindFlag.ENABLED ? this.channelBindMethod : null;

    this.clientFirstMessage = new ClientFirstMessage(gs2CbindFlag, authzid, channelBindMethod, user, nonce);

    return clientFirstMessage.toString().getBytes(UTF_8);
  }

  /**
   * Definitive answer to whether the client should be providing channel-bind
   * data to the {@link #receiveServerFirstMessage} method call.
   * @return True if the client should provide channel-bind data.
   */
  public boolean requiresChannelBindData() {
    return scramMechanism.requiresChannelBinding();
  }

  /**
   * Selected method the client should use to generate channel-bind data
   * @return Name of channel-bind method
   */
  public String getChannelBindMethod() {
    return channelBindMethod;
  }

  /**
   * Generates a client-final-message from the received server-first-message, channel-bind data (if any),
   * and the user's password. A matching {@link ScramSession.ClientFinalProcessor} is stored internally for a
   * later call to {@link #receiveServerFinalMessage(String)} to complete the authentication.
   *
   * @param serverFirstMessage The message
   * @param channelBindData Optional channel-bind data (my be null)
   * @param password The user's password
   * @return The generated client-final-message.
   * @throws ScramParseException If the message is not a valid server-first-message
   */
  public byte[] receiveServerFirstMessage(String serverFirstMessage, byte[] channelBindData, String password) throws ScramException {
    if (requiresChannelBindData() && channelBindData == null) {
      throw new ScramException("Missing required channel-bind data");
    }

    clientFinalProcessor =
        new ServerFirstProcessor(checkNotEmpty(serverFirstMessage, "serverFirstMessage"))
            .clientFinalProcessor(password);
    return clientFinalProcessor.clientFinalMessage(channelBindData).getBytes(UTF_8);
  }

  /**
   * Generates a client-final-message from the received server-first-message, channel-bind data (if any),
   * and the clientKey and storedKey which, if available, provide an optimized path versus providing the original
   * user's passwordthe user's password. A matching {@link ScramSession.ClientFinalProcessor} is stored internally
   * for a later call to {@link #receiveServerFinalMessage(String)} to complete the authentication.
   *
   * @param serverFirstMessage The message
   * @param channelBindData Optional channel-bind data (my be null)
   * @param clientKey The client key, as per the SCRAM algorithm.
   *                  It can be generated with:
   *                  {@link ScramFunctions#clientKey(ScramMechanism, StringPreparation, String, byte[], int)}
   * @param storedKey The stored key, as per the SCRAM algorithm.
   *                  It can be generated from the client key with:
   *                  {@link ScramFunctions#storedKey(ScramMechanism, byte[])}
   * @return The generated client-final-message.
   * @throws ScramParseException If the message is not a valid server-first-message
   */
  public byte[] receiveServerFirstMessage(String serverFirstMessage, byte[] channelBindData,
                                          byte[] clientKey, byte[] storedKey) throws ScramException {
    if (requiresChannelBindData() && channelBindData == null) {
      throw new ScramException("Missing required channel-bind data");
    }

    clientFinalProcessor =
        new ServerFirstProcessor(checkNotEmpty(serverFirstMessage, "serverFirstMessage"))
            .clientFinalProcessor(clientKey, storedKey);
    return clientFinalProcessor.clientFinalMessage(channelBindData).getBytes(UTF_8);
  }

  public void receiveServerFinalMessage(String serverFinalMessage) throws ScramException {
    if (clientFinalProcessor == null) {
      throw new IllegalStateException("No ClientFinalProcessor selected. Ensure receiveServerFirstMessage has been called.");
    }
    clientFinalProcessor.receiveServerFinalMessage(serverFinalMessage);
  }

  /**
   * Process a received server-first-message.
   */
  private class ServerFirstProcessor {
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
   * Processor that allows to generate the client-final-message, as well as process the
   * server-final-message and verify server's signature.
   */
  private class ClientFinalProcessor {
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
     * @param cbindData Optional channel-binding data
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

}
