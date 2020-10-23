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


import com.impossibl.postgres.protocol.sasl.scram.util.StringWritable;
import com.impossibl.postgres.protocol.sasl.scram.util.StringWritableCsv;

import com.impossibl.postgres.protocol.sasl.scram.ScramAttributeValue;
import com.impossibl.postgres.protocol.sasl.scram.ScramAttributes;
import com.impossibl.postgres.protocol.sasl.scram.ScramStringFormatting;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramParseException;
import com.impossibl.postgres.protocol.sasl.scram.gssapi.Gs2CbindFlag;
import com.impossibl.postgres.protocol.sasl.scram.gssapi.Gs2Header;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotEmpty;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;


/**
 * Constructs and parses client-first-messages.
 * Message contains a {@link Gs2Header}, a username and a nonce. Formal syntax is:
 *
 * {@code
 * client-first-message-bare = [reserved-mext ","] username "," nonce ["," extensions]
   client-first-message = gs2-header client-first-message-bare
 * }
 *
 * Note that extensions are not supported.
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802#section-7">[RFC5802] Section 7</a>
 */
public class ClientFirstMessage implements StringWritable {
    private final Gs2Header gs2Header;
    private final String user;
    private final String nonce;

    /**
     * Constructs a client-first-message for the given user, nonce and gs2Header.
     * This constructor is intended to be instantiated by a scram client, and not directly.
     * The client should be providing the header, and nonce (and probably the user too).
     * @param gs2Header The GSS-API header
     * @param user The SCRAM user
     * @param nonce The nonce for this session
     * @throws IllegalArgumentException If any of the arguments is null or empty
     */
    public ClientFirstMessage(Gs2Header gs2Header, String user, String nonce) throws IllegalArgumentException {
        this.gs2Header = checkNotNull(gs2Header, "gs2Header");
        this.user = checkNotEmpty(user, "user");
        this.nonce = checkNotEmpty(nonce, "nonce");
    }

    private static Gs2Header gs2Header(Gs2CbindFlag gs2CbindFlag, String authzid, String cbindName) {
        checkNotNull(gs2CbindFlag, "gs2CbindFlag");
        if(Gs2CbindFlag.CHANNEL_BINDING_REQUIRED == gs2CbindFlag && null == cbindName) {
            throw new IllegalArgumentException("Channel binding name is required if channel binding is specified");
        }

        return new Gs2Header(gs2CbindFlag, cbindName, authzid);
    }

    /**
     * Constructs a client-first-message for the given parameters.
     * Under normal operation, this constructor is intended to be instantiated by a scram client, and not directly.
     * However, this constructor is more user- or test-friendly, as the arguments are easier to provide without
     * building other indirect object parameters.
     * @param gs2CbindFlag The channel-binding flag
     * @param authzid The optional authzid
     * @param cbindName The optional channel binding name
     * @param user The SCRAM user
     * @param nonce The nonce for this session
     * @throws IllegalArgumentException If the flag, user or nonce are null or empty
     */
    public ClientFirstMessage(Gs2CbindFlag gs2CbindFlag, String authzid, String cbindName, String user, String nonce) {
        this(gs2Header(gs2CbindFlag, authzid, cbindName), user, nonce);
    }

    /**
     * Constructs a client-first-message for the given parameters, with no channel binding nor authzid.
     * Under normal operation, this constructor is intended to be instantiated by a scram client, and not directly.
     * However, this constructor is more user- or test-friendly, as the arguments are easier to provide without
     * building other indirect object parameters.
     * @param user The SCRAM user
     * @param nonce The nonce for this session
     * @throws IllegalArgumentException If the user or nonce are null or empty
     */
    public ClientFirstMessage(String user, String nonce) {
        this(gs2Header(Gs2CbindFlag.CLIENT_NOT, null, null), user, nonce);
    }

    public Gs2CbindFlag getChannelBindingFlag() {
        return gs2Header.getChannelBindingFlag();
    }

    public boolean isChannelBinding() {
        return gs2Header.getChannelBindingFlag() == Gs2CbindFlag.CHANNEL_BINDING_REQUIRED;
    }

    public String getChannelBindingName() {
        return gs2Header.getChannelBindingName();
    }

    public String getAuthzid() {
        return gs2Header.getAuthzid();
    }

    public Gs2Header getGs2Header() {
        return gs2Header;
    }

    public String getUser() {
        return user;
    }

    public String getNonce() {
        return nonce;
    }

    /**
     * Limited version of the {@link StringWritableCsv#toString()} method, that doesn't write the GS2 header.
     * This method is useful to construct the auth message used as part of the SCRAM algorithm.
     * @param sb A StringBuffer where to write the data to.
     * @return The same StringBuffer
     */
    public StringBuffer writeToWithoutGs2Header(StringBuffer sb) {
        return StringWritableCsv.writeTo(
                sb,
                new ScramAttributeValue(ScramAttributes.USERNAME, ScramStringFormatting.toSaslName(user)),
                new ScramAttributeValue(ScramAttributes.NONCE, nonce)
        );
    }

    @Override
    public StringBuffer writeTo(StringBuffer sb) {
        StringWritableCsv.writeTo(
                sb,
                gs2Header,
                null    // This marks the position of the rest of the elements, required for the ","
        );

        return writeToWithoutGs2Header(sb);
    }

    /**
     * Construct a {@link ClientFirstMessage} instance from a message (String)
     * @param clientFirstMessage The String representing the client-first-message
     * @return The instance
     * @throws ScramParseException If the message is not a valid client-first-message
     * @throws IllegalArgumentException If the message is null or empty
     */
    public static ClientFirstMessage parseFrom(String clientFirstMessage)
    throws ScramParseException, IllegalArgumentException {
        checkNotEmpty(clientFirstMessage, "clientFirstMessage");

        Gs2Header gs2Header = Gs2Header.parseFrom(clientFirstMessage);  // Takes first two fields
        String[] userNonceString;
        try {
            userNonceString = StringWritableCsv.parseFrom(clientFirstMessage, 2, 2);
        } catch (IllegalArgumentException e) {
            throw new ScramParseException("Illegal series of attributes in client-first-message", e);
        }

        ScramAttributeValue user = ScramAttributeValue.parse(userNonceString[0]);
        if(ScramAttributes.USERNAME.getChar() != user.getChar()) {
            throw new ScramParseException("user must be the 3rd element of the client-first-message");
        }

        ScramAttributeValue nonce = ScramAttributeValue.parse(userNonceString[1]);
        if(ScramAttributes.NONCE.getChar() != nonce.getChar()) {
            throw new ScramParseException("nonce must be the 4th element of the client-first-message");
        }

        return new ClientFirstMessage(gs2Header, user.getValue(), nonce.getValue());
    }

    @Override
    public String toString() {
        return writeTo(new StringBuffer()).toString();
    }
}
