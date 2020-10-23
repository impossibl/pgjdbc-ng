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


package com.impossibl.postgres.protocol.sasl.scram.util;


import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkArgument;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;


/**
 * Utility static methods for cryptography related tasks.
 */
public class CryptoUtil {
    private static final int MIN_ASCII_PRINTABLE_RANGE = 0x21;
    private static final int MAX_ASCII_PRINTABLE_RANGE = 0x7e;
    private static final int EXCLUDED_CHAR = (int) ','; // 0x2c

    private static class SecureRandomHolder {
        private static final SecureRandom INSTANCE = new SecureRandom();
    }

    /**
     * Generates a random string (called a 'nonce'), composed of ASCII printable characters, except comma (',').
     * @param size The length of the nonce, in characters/bytes
     * @param random The SecureRandom to use
     * @return The String representing the nonce
     */
    public static String nonce(int size, SecureRandom random) {
        if(size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }

        char[] chars = new char[size];
        int r;
        for(int i = 0; i < size;) {
            r = random.nextInt(MAX_ASCII_PRINTABLE_RANGE - MIN_ASCII_PRINTABLE_RANGE + 1) + MIN_ASCII_PRINTABLE_RANGE;
            if(r != EXCLUDED_CHAR) {
                chars[i++] = (char) r;
            }
        }

        return new String(chars);
    }

    /**
     * Generates a random string (called a 'nonce'), composed of ASCII printable characters, except comma (',').
     * It uses a default SecureRandom instance.
     * @param size The length of the nonce, in characters/bytes
     * @return The String representing the nonce
     */
    public static String nonce(int size) {
        return nonce(size, SecureRandomHolder.INSTANCE);
    }

    /**
     * Compute the "Hi" function for SCRAM.
     *
     * {@code
     * Hi(str, salt, i):
     *
     *      U1   := HMAC(str, salt + INT(1))
     *      U2   := HMAC(str, U1)
     *      ...
     *      Ui-1 := HMAC(str, Ui-2)
     *      Ui   := HMAC(str, Ui-1)
     *
     *      Hi := U1 XOR U2 XOR ... XOR Ui
     *
     *       where "i" is the iteration count, "+" is the string concatenation
     *       operator, and INT(g) is a 4-octet encoding of the integer g, most
     *       significant octet first.
     *
     *       Hi() is, essentially, PBKDF2 [RFC2898] with HMAC() as the
     *       pseudorandom function (PRF) and with dkLen == output length of
     *       HMAC() == output length of H().
     * }
     *
     * @param secretKeyFactory The SecretKeyFactory to generate the SecretKey
     * @param keyLength The length of the key (in bits)
     * @param value The char array to compute the Hi function
     * @param salt The salt
     * @param iterations The number of iterations
     * @return The bytes of the computed Hi value
     */
    public static byte[] hi(
            SecretKeyFactory secretKeyFactory, int keyLength, char[] value, byte[] salt, int iterations
    ) {
        try {
            PBEKeySpec spec = new PBEKeySpec(value, salt, iterations, keyLength);
            SecretKey key = secretKeyFactory.generateSecret(spec);
            return key.getEncoded();
        } catch(InvalidKeySpecException e) {
            throw new RuntimeException("Platform error: unsupported PBEKeySpec");
        }
    }

    /**
     * Computes the HMAC of a given message.
     *
     * {@code
     * HMAC(key, str): Apply the HMAC keyed hash algorithm (defined in
     * [RFC2104]) using the octet string represented by "key" as the key
     * and the octet string "str" as the input string.  The size of the
     * result is the hash result size for the hash function in use.  For
     * example, it is 20 octets for SHA-1 (see [RFC3174]).
     * }
     *
     * @param secretKeySpec A key of the given algorithm
     * @param mac A MAC instance of the given algorithm
     * @param message The message to compute the HMAC
     * @return The bytes of the computed HMAC value
     */
    public static byte[] hmac(SecretKeySpec secretKeySpec, Mac mac, byte[] message) {
        try {
            mac.init(secretKeySpec);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("Platform error: unsupported key for HMAC algorithm");
        }

        return mac.doFinal(message);
    }

    /**
     * Computes a byte-by-byte xor operation.
     *
     * {@code
     * XOR: Apply the exclusive-or operation to combine the octet string
     * on the left of this operator with the octet string on the right of
     * this operator.  The length of the output and each of the two
     * inputs will be the same for this use.
     * }
     *
     * @param value1
     * @param value2
     * @return
     * @throws IllegalArgumentException
     */
    public static byte[] xor(byte[] value1, byte[] value2) throws IllegalArgumentException {
        checkNotNull(value1, "value1");
        checkNotNull(value2, "value2");
        checkArgument(value1.length == value2.length, "Both values must have the same length");

        byte[] result = new byte[value1.length];
        for(int i = 0; i < value1.length; i++) {
            result[i] = (byte) (value1[i] ^ value2[i]);
        }

        return result;
    }
}
