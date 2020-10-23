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
import com.impossibl.postgres.protocol.sasl.scram.ScramStringFormatting;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramParseException;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritable;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritableCsv;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotEmpty;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;


/**
 * Constructs and parses server-final-messages. Formal syntax is:
 *
 * {@code
 *    server-error = "e=" server-error-value
 *
 *    server-error-value = "invalid-encoding" /
 *                   "extensions-not-supported" /  ; unrecognized 'm' value
 *                   "invalid-proof" /
 *                   "channel-bindings-dont-match" /
 *                   "server-does-support-channel-binding" /
 *                     ; server does not support channel binding
 *                   "channel-binding-not-supported" /
 *                   "unsupported-channel-binding-type" /
 *                   "unknown-user" /
 *                   "invalid-username-encoding" /
 *                     ; invalid username encoding (invalid UTF-8 or
 *                     ; SASLprep failed)
 *                   "no-resources" /
 *                   "other-error" /
 *                   server-error-value-ext
 *            ; Unrecognized errors should be treated as "other-error".
 *            ; In order to prevent information disclosure, the server
 *            ; may substitute the real reason with "other-error".
 *
 *    server-error-value-ext = value
 *            ; Additional error reasons added by extensions
 *            ; to this document.
 *
 *    verifier        = "v=" base64
 *                      ;; base-64 encoded ServerSignature.
 *
 *    server-final-errorMessage = (server-error / verifier)
 *                      ["," extensions]
 * }
 *
 * Note that extensions are not supported (and, consequently, error message extensions).
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802#section-7">[RFC5802] Section 7</a>
 */
public class ServerFinalMessage implements StringWritable {

  /**
   * Possible error messages sent on a server-final-message.
   */
  public enum Error {
    INVALID_ENCODING("invalid-encoding"),
    EXTENSIONS_NOT_SUPPORTED("extensions-not-supported"),
    INVALID_PROOF("invalid-proof"),
    CHANNEL_BINDINGS_DONT_MATCH("channel-bindings-dont-match"),
    SERVER_DOES_SUPPORT_CHANNEL_BINDING("server-does-support-channel-binding"),
    CHANNEL_BINDING_NOT_SUPPORTED("channel-binding-not-supported"),
    UNSUPPORTED_CHANNEL_BINDING_TYPE("unsupported-channel-binding-type"),
    UNKNOWN_USER("unknown-user"),
    INVALID_USERNAME_ENCODING("invalid-username-encoding"),
    NO_RESOURCES("no-resources"),
    OTHER_ERROR("other-error");

    private static final Map<String, Error> BY_NAME_MAPPING = valuesAsMap();

    private final String errorMessage;

    Error(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public static Error getByErrorMessage(String errorMessage) throws IllegalArgumentException {
      checkNotEmpty(errorMessage, "errorMessage");

      if (!BY_NAME_MAPPING.containsKey(errorMessage)) {
        throw new IllegalArgumentException("Invalid error message '" + errorMessage + "'");
      }

      return BY_NAME_MAPPING.get(errorMessage);
    }

    private static Map<String, Error> valuesAsMap() {
      Map<String, Error> map = new HashMap<>(values().length);
      for (Error error : values()) {
        map.put(error.errorMessage, error);
      }
      return map;
    }

  }

  private final byte[] verifier;
  private final Error error;

  /**
   * Constructs a server-final-message with no errors, and the provided server verifier
   * @param verifier The bytes of the computed signature
   * @throws IllegalArgumentException If the verifier is null
   */
  public ServerFinalMessage(byte[] verifier) throws IllegalArgumentException {
    this.verifier = checkNotNull(verifier, "verifier");
    this.error = null;
  }

  /**
   * Constructs a server-final-message which represents a SCRAM error.
   * @param error The error
   * @throws IllegalArgumentException If the error is null
   */
  public ServerFinalMessage(Error error) throws IllegalArgumentException {
    this.error = checkNotNull(error, "error");
    this.verifier = null;
  }

  /**
   * Whether this server-final-message contains an error
   * @return True if it contains an error, false if it contains a verifier
   */
  public boolean isError() {
    return null != error;
  }

  public byte[] getVerifier() {
    return verifier;
  }

  public Error getError() {
    return error;
  }

  @Override
  public StringBuffer writeTo(StringBuffer sb) {
    return StringWritableCsv.writeTo(
        sb,
        isError() ?
            new ScramAttributeValue(ScramAttributes.ERROR, error.errorMessage)
            : new ScramAttributeValue(
            ScramAttributes.SERVER_SIGNATURE, ScramStringFormatting.base64Encode(verifier)
        )
    );
  }

  /**
   * Parses a server-final-message from a String.
   * @param serverFinalMessage The message
   * @return A constructed server-final-message instance
   * @throws ScramParseException If the argument is not a valid server-final-message
   * @throws IllegalArgumentException If the message is null or empty
   */
  public static ServerFinalMessage parseFrom(String serverFinalMessage)
      throws ScramParseException, IllegalArgumentException {
    checkNotEmpty(serverFinalMessage, "serverFinalMessage");

    String[] attributeValues = StringWritableCsv.parseFrom(serverFinalMessage, 1, 0);
    if (attributeValues == null || attributeValues.length != 1) {
      throw new ScramParseException("Invalid server-final-message");
    }

    ScramAttributeValue attributeValue = ScramAttributeValue.parse(attributeValues[0]);
    if (ScramAttributes.SERVER_SIGNATURE.getChar() == attributeValue.getChar()) {
      byte[] verifier = ScramStringFormatting.base64Decode(attributeValue.getValue());
      return new ServerFinalMessage(verifier);
    }
    else if (ScramAttributes.ERROR.getChar() == attributeValue.getChar()) {
      return new ServerFinalMessage(Error.getByErrorMessage(attributeValue.getValue()));
    }
    else {
      throw new ScramParseException(
          "Invalid server-final-message: it must contain either a verifier or an error attribute"
      );
    }
  }

  @Override
  public String toString() {
    return writeTo(new StringBuffer()).toString();
  }
}
