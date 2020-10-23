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


import com.impossibl.postgres.protocol.sasl.scram.stringprep.StringPreparation;
import com.impossibl.postgres.protocol.sasl.scram.util.CryptoUtil;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.gt0;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.SecretKeySpec;

/**
 * SCRAM Mechanisms supported by this library.
 * At least, SCRAM-SHA-1 and SCRAM-SHA-256 are provided, since both the hash and the HMAC implementations
 * are provided by the Java JDK version 6 or greater.
 *
 * {@link MessageDigest}: "Every implementation of the Java platform is required to support the
 * following standard MessageDigest algorithms: MD5, SHA-1, SHA-256".
 *
 * {@link Mac}: "Every implementation of the Java platform is required to support the following
 * standard Mac algorithms: HmacMD5, HmacSHA1, HmacSHA256".
 *
 * @see <a href="https://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml#scram">
 *      SASL SCRAM Family Mechanisms</a>
 */
public enum ScramMechanisms implements ScramMechanism {
  SCRAM_SHA_1("SHA-1", "SHA-1", 160, "HmacSHA1", false),
  SCRAM_SHA_1_PLUS("SHA-1", "SHA-1", 160, "HmacSHA1", true),
  SCRAM_SHA_256("SHA-256", "SHA-256", 256, "HmacSHA256", false),
  SCRAM_SHA_256_PLUS("SHA-256", "SHA-256", 256, "HmacSHA256", true);

  private static final String NAME_PREFIX = "SCRAM-";
  private static final String CHANNEL_BINDING_SUFFIX = "-PLUS";
  private static final String PBKDF2_ALGORITHM_PREFIX = "PBKDF2With";
  private static final Map<String, ScramMechanisms> BY_NAME_MAPPING = valuesAsMap();

  private final String mechanismName;
  private final String hashAlgorithmName;
  private final int keyLength;
  private final String hmacAlgorithmName;
  private final boolean channelBinding;

  ScramMechanisms(String name, String hashAlgorithmName, int keyLength,
                  String hmacAlgorithmName, boolean channelBinding) {
    this.mechanismName = NAME_PREFIX + checkNotNull(name, "name") + (channelBinding ? CHANNEL_BINDING_SUFFIX : "");
    this.hashAlgorithmName = checkNotNull(hashAlgorithmName, "hashAlgorithmName");
    this.keyLength = gt0(keyLength, "keyLength");
    this.hmacAlgorithmName = checkNotNull(hmacAlgorithmName, "hmacAlgorithmName");
    this.channelBinding = channelBinding;
  }

  /**
   * Method that returns the name of the hash algorithm.
   * It is protected since should be of no interest for direct users.
   * The instance is supposed to provide abstractions over the algorithm names,
   * and are not meant to be directly exposed.
   * @return The name of the hash algorithm
   */
  protected String getHashAlgorithmName() {
    return hashAlgorithmName;
  }

  /**
   * Method that returns the name of the HMAC algorithm.
   * It is protected since should be of no interest for direct users.
   * The instance is supposed to provide abstractions over the algorithm names,
   * and are not meant to be directly exposed.
   * @return The name of the HMAC algorithm
   */
  protected String getHmacAlgorithmName() {
    return hmacAlgorithmName;
  }

  @Override
  public String getName() {
    return mechanismName;
  }

  @Override
  public boolean requiresChannelBinding() {
    return channelBinding;
  }

  @Override
  public int algorithmKeyLength() {
    return keyLength;
  }

  @Override
  public byte[] digest(byte[] message) {
    try {
      return MessageDigest.getInstance(hashAlgorithmName).digest(message);
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Algorithm " + hashAlgorithmName + " not present in current JVM");
    }
  }

  @Override
  public byte[] hmac(byte[] key, byte[] message) {
    try {
      return CryptoUtil.hmac(new SecretKeySpec(key, hmacAlgorithmName), Mac.getInstance(hmacAlgorithmName), message);
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MAC Algorithm " + hmacAlgorithmName + " not present in current JVM");
    }
  }

  @Override
  public byte[] saltedPassword(StringPreparation stringPreparation, String password, byte[] salt, int iterations) {
    String keyFactoryAlgorithmName = PBKDF2_ALGORITHM_PREFIX + hmacAlgorithmName;
    char[] normalizedString = stringPreparation.normalize(password).toCharArray();
    try {
      return CryptoUtil.hi(
          SecretKeyFactory.getInstance(keyFactoryAlgorithmName),
          algorithmKeyLength(),
          normalizedString,
          salt,
          iterations
      );
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("PBKDF Algorithm " + keyFactoryAlgorithmName + " not present in current JVM");
    }
  }

  /**
   * Gets a SCRAM mechanism, given its standard IANA name.
   * @param name The standard IANA full name of the mechanism.
   * @return An Optional instance that contains the ScramMechanism if it was found, or empty otherwise.
   */
  public static ScramMechanisms byName(String name) {
    checkNotNull(name, "name");

    return BY_NAME_MAPPING.get(name);
  }

  private static Map<String, ScramMechanisms> valuesAsMap() {
    Map<String, ScramMechanisms> mapScramMechanisms = new HashMap<>(values().length);
    for (ScramMechanisms scramMechanisms : values()) {
      mapScramMechanisms.put(scramMechanisms.getName(), scramMechanisms);
    }
    return mapScramMechanisms;
  }

}
