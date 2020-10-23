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
import com.impossibl.postgres.protocol.sasl.scram.gssapi.Gs2Header;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritable;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritableCsv;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotEmpty;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;

import static java.nio.charset.StandardCharsets.US_ASCII;


/**
 * Constructs and parses client-final-messages. Formal syntax is:
 *
 * {@code
 * client-final-message-without-proof = channel-binding "," nonce ["," extensions]
 * client-final-message = client-final-message-without-proof "," proof
 * }
 *
 * Note that extensions are not supported.
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802#section-7">[RFC5802] Section 7</a>
 */
public class ClientFinalMessage implements StringWritable {
  private final byte[] cbindInput;
  private final String nonce;
  private final byte[] proof;

  private static byte[] generateCBindInput(Gs2Header gs2Header, byte[] cbindData) {
    StringBuffer sb = new StringBuffer();
    gs2Header.writeTo(sb)
        .append(',');

    byte[] cbindInput = sb.toString().getBytes(US_ASCII);

    if (null != cbindData) {
      byte[] cbindInputNew = new byte[cbindInput.length + cbindData.length];
      System.arraycopy(cbindInput, 0, cbindInputNew, 0, cbindInput.length);
      System.arraycopy(cbindData, 0, cbindInputNew, cbindInput.length, cbindData.length);
      cbindInput = cbindInputNew;
    }

    return cbindInput;
  }

  /**
   * Constructus a client-final-message with the provided gs2Header (the same one used in the client-first-message),
   * optionally the channel binding data, and the nonce.
   * This method is intended to be used by SCRAM clients, and not to be constructed directly.
   * @param gs2Header The GSS-API header
   * @param cbindData If using channel binding, the channel binding data
   * @param nonce The nonce
   * @param proof The bytes representing the computed client proof
   */
  public ClientFinalMessage(Gs2Header gs2Header, byte[] cbindData, String nonce, byte[] proof) {
    this.cbindInput = generateCBindInput(checkNotNull(gs2Header, "gs2Header"), cbindData);
    this.nonce = checkNotEmpty(nonce, "nonce");
    this.proof = checkNotNull(proof, "proof");
  }

  private static StringBuffer writeToWithoutProof(StringBuffer sb, byte[] cbindInput, String nonce) {
    return StringWritableCsv.writeTo(
        sb,
        new ScramAttributeValue(ScramAttributes.CHANNEL_BINDING, ScramStringFormatting.base64Encode(cbindInput)),
        new ScramAttributeValue(ScramAttributes.NONCE, nonce)
    );
  }

  private static StringBuffer writeToWithoutProof(StringBuffer sb, Gs2Header gs2Header, byte[] cbindData, String nonce) {
    return writeToWithoutProof(
        sb,
        generateCBindInput(checkNotNull(gs2Header, "gs2Header"), cbindData),
        nonce
    );
  }

  /**
   * Returns a StringBuffer filled in with the formatted output of a client-first-message without the proof value.
   * This is useful for computing the auth-message, used in turn to compute the proof.
   * @param gs2Header The GSS-API header
   * @param cbindData The optional channel binding data
   * @param nonce The nonce
   * @return The String representation of the part of the message that excludes the proof
   */
  public static StringBuffer writeToWithoutProof(Gs2Header gs2Header, byte[] cbindData, String nonce) {
    return writeToWithoutProof(new StringBuffer(), gs2Header, cbindData, nonce);
  }

  @Override
  public StringBuffer writeTo(StringBuffer sb) {
    writeToWithoutProof(sb, cbindInput, nonce);

    return StringWritableCsv.writeTo(
        sb,
        null,   // This marks the position of writeToWithoutProof, required for the ","
        new ScramAttributeValue(ScramAttributes.CLIENT_PROOF, ScramStringFormatting.base64Encode(proof))
    );
  }

  @Override
  public String toString() {
    return writeTo(new StringBuffer()).toString();
  }
}
