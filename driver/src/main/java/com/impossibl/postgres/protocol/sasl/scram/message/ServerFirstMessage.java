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


package com.impossibl.postgres.protocol.sasl.scram.message;


import com.impossibl.postgres.protocol.sasl.scram.ScramAttributeValue;
import com.impossibl.postgres.protocol.sasl.scram.ScramAttributes;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramParseException;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritable;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritableCsv;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkArgument;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotEmpty;


/**
 * Constructs and parses server-first-messages. Formal syntax is:
 *
 * {@code
 * server-first-message = [reserved-mext ","] nonce "," salt ","
 *                        iteration-count ["," extensions]
 * }
 *
 * Note that extensions are not supported.
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802#section-7">[RFC5802] Section 7</a>
 */
public class ServerFirstMessage implements StringWritable {
  /**
   * Minimum allowed value for the iteration, as per the RFC.
   */
  public static final int ITERATION_MIN_VALUE = 4096;

  private final String clientNonce;
  private final String serverNonce;
  private final String salt;
  private final int iteration;

  /**
   * Constructs a server-first-message from a client-first-message and the additional required data.
   * @param clientNonce String representing the client-first-message
   * @param serverNonce Server serverNonce
   * @param salt The salt
   * @param iteration The iteration count (must be &lt;= 4096)
   * @throws IllegalArgumentException If clientFirstMessage, serverNonce or salt are null or empty,
   *                                  or iteration &lt; 4096
   */
  public ServerFirstMessage(
      String clientNonce, String serverNonce, String salt, int iteration
  ) throws IllegalArgumentException {
    this.clientNonce = checkNotEmpty(clientNonce, "clientNonce");
    this.serverNonce = checkNotEmpty(serverNonce, "serverNonce");
    this.salt = checkNotEmpty(salt, "salt");
    checkArgument(iteration >= ITERATION_MIN_VALUE, "iteration must be >= " + ITERATION_MIN_VALUE);
    this.iteration = iteration;
  }

  public String getClientNonce() {
    return clientNonce;
  }

  public String getServerNonce() {
    return serverNonce;
  }

  public String getNonce() {
    return clientNonce + serverNonce;
  }

  public String getSalt() {
    return salt;
  }

  public int getIteration() {
    return iteration;
  }

  @Override
  public StringBuffer writeTo(StringBuffer sb) {
    return StringWritableCsv.writeTo(
        sb,
        new ScramAttributeValue(ScramAttributes.NONCE, getNonce()),
        new ScramAttributeValue(ScramAttributes.SALT, salt),
        new ScramAttributeValue(ScramAttributes.ITERATION, iteration + "")
    );
  }

  /**
   * Parses a server-first-message from a String.
   * @param serverFirstMessage The string representing the server-first-message
   * @param clientNonce The serverNonce that is present in the client-first-message
   * @return The parsed instance
   * @throws ScramParseException If the argument is not a valid server-first-message
   * @throws IllegalArgumentException If either argument is empty or serverFirstMessage is not a valid message
   */
  public static ServerFirstMessage parseFrom(String serverFirstMessage, String clientNonce)
      throws ScramParseException, IllegalArgumentException {
    checkNotEmpty(serverFirstMessage, "serverFirstMessage");
    checkNotEmpty(clientNonce, "clientNonce");

    String[] attributeValues = StringWritableCsv.parseFrom(serverFirstMessage, 3, 0);
    if (attributeValues.length != 3) {
      throw new ScramParseException("Invalid server-first-message");
    }

    ScramAttributeValue nonce = ScramAttributeValue.parse(attributeValues[0]);
    if (ScramAttributes.NONCE.getChar() != nonce.getChar()) {
      throw new ScramParseException("serverNonce must be the 1st element of the server-first-message");
    }
    if (!nonce.getValue().startsWith(clientNonce)) {
      throw new ScramParseException("parsed serverNonce does not start with client serverNonce");
    }

    ScramAttributeValue salt = ScramAttributeValue.parse(attributeValues[1]);
    if (ScramAttributes.SALT.getChar() != salt.getChar()) {
      throw new ScramParseException("salt must be the 2nd element of the server-first-message");
    }

    ScramAttributeValue iteration = ScramAttributeValue.parse(attributeValues[2]);
    if (ScramAttributes.ITERATION.getChar() != iteration.getChar()) {
      throw new ScramParseException("iteration must be the 3rd element of the server-first-message");
    }

    int iterationInt;
    try {
      iterationInt = Integer.parseInt(iteration.getValue());
    }
    catch (NumberFormatException e) {
      throw new ScramParseException("invalid iteration");
    }

    return new ServerFirstMessage(
        clientNonce, nonce.getValue().substring(clientNonce.length()), salt.getValue(), iterationInt
    );
  }

  @Override
  public String toString() {
    return writeTo(new StringBuffer()).toString();
  }
}
