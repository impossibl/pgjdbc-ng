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


package com.impossibl.postgres.protocol.sasl.scram;


import com.impossibl.postgres.jdbc.xa.Base64;

import java.nio.charset.StandardCharsets;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotEmpty;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;

/**
 * Class with static methods that provide support for converting to/from salNames.
 * @see <a href="https://tools.ietf.org/html/rfc5802#section-7">[RFC5802] Section 7: Formal Syntax</a>
 */
public class ScramStringFormatting {

    /**
     * Given a value-safe-char (normalized UTF-8 String),
     * return one where characters ',' and '=' are represented by '=2C' or '=3D', respectively.
     * @param value The value to convert so saslName
     * @return The saslName, with caracter escaped (if any)
     */
    public static String toSaslName(String value) {
        if(null == value || value.isEmpty()) {
            return value;
        }

        int nComma = 0, nEqual = 0;
        char[] originalChars = value.toCharArray();

        // Fast path
        for(char c : originalChars) {
            if(',' == c) { nComma++; }
            else if('=' == c) { nEqual++; }
        }
        if(nComma == 0 && nEqual == 0) {
            return value;
        }

        // Replace chars
        char[] saslChars = new char[originalChars.length + nComma * 2 + nEqual * 2];
        int i = 0;
        for(char c : originalChars) {
            if(',' == c) {
                saslChars[i++] = '=';
                saslChars[i++] = '2';
                saslChars[i++] = 'C';
            } else if('=' == c) {
                saslChars[i++] = '=';
                saslChars[i++] = '3';
                saslChars[i++] = 'D';
            } else {
                saslChars[i++] = c;
            }
        }

        return new String(saslChars);
    }

    /**
     * Given a saslName, return a non-escaped String.
     * @param value The saslName
     * @return The saslName, unescaped
     * @throws IllegalArgumentException If a ',' character is present, or a '=' not followed by either '2C' or '3D'
     */
    public static String fromSaslName(String value) throws IllegalArgumentException {
        if(null == value || value.isEmpty()) {
            return value;
        }

        int nEqual = 0;
        char[] orig = value.toCharArray();

        // Fast path
        for(int i = 0; i < orig.length; i++) {
            if(orig[i] == ',') {
                throw new IllegalArgumentException("Invalid ',' character present in saslName");
            }
            if(orig[i] == '=') {
                nEqual++;
                if(i + 2 > orig.length - 1) {
                    throw new IllegalArgumentException("Invalid '=' character present in saslName");
                }
                if(! (orig[i+1] == '2' && orig[i+2] == 'C' || orig[i+1] == '3' && orig[i+2] == 'D')) {
                    throw new IllegalArgumentException(
                            "Invalid char '=" + orig[i+1] + orig[i+2] + "' found in saslName"
                    );
                }
            }
        }
        if(nEqual == 0) {
            return value;
        }

        // Replace characters
        char[] replaced = new char[orig.length - nEqual * 2];

        for(int r = 0, o = 0; r < replaced.length; r++) {
            if('=' == orig[o]) {
                if(orig[o+1] == '2' && orig[o+2] == 'C') {
                    replaced[r] = ',';
                } else if(orig[o+1] == '3' && orig[o+2] == 'D') {
                    replaced[r] = '=';
                }
                o += 3;
            } else {
                replaced[r] = orig[o];
                o += 1;
            }
        }

        return new String(replaced);
    }

    public static String base64Encode(byte[] value) throws IllegalArgumentException {
        return Base64.encodeBytes(checkNotNull(value, "value"));
    }

    public static String base64Encode(String value) throws IllegalArgumentException {
        return base64Encode(checkNotEmpty(value, "value").getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] base64Decode(String value) throws IllegalArgumentException {
        return Base64.decode(checkNotEmpty(value, "value"));
    }
}
