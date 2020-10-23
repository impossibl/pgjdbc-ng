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


/**
 * Definition of the functionality to be provided by every ScramMechanism.
 *
 * Every ScramMechanism implemented must provide implementations of their respective digest and hmac
 * function that will not throw a RuntimeException on any JVM, to guarantee true portability of this library.
 */
public interface ScramMechanism {
  /**
   * The name of the mechanism, which must be a value registered under IANA:
   * <a href="https://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml#scram">
   *      SASL SCRAM Family Mechanisms</a>
   * @return The mechanism name
   */
  String getName();

  /**
   * Calculate a message digest, according to the algorithm of the SCRAM mechanism.
   * @param message the message
   * @return The calculated message digest
   * @throws RuntimeException If the algorithm is not provided by current JVM or any included implementations
   */
  byte[] digest(byte[] message) throws RuntimeException;

  /**
   * Calculate the hmac of a key and a message, according to the algorithm of the SCRAM mechanism.
   * @param key the key
   * @param message the message
   * @return The calculated message hmac instance
   * @throws RuntimeException If the algorithm is not provided by current JVM or any included implementations
   */
  byte[] hmac(byte[] key, byte[] message) throws RuntimeException;

  /**
   * Returns the length of the key length  of the algorithm.
   * @return The length (in bits)
   */
  int algorithmKeyLength();

  /**
   * Whether this mechanism requires channel binding
   * @return True if it supports channel binding, false otherwise
   */
  boolean requiresChannelBinding();

  /**
   * Compute the salted password
   * @return The salted password
   */
  byte[] saltedPassword(StringPreparation stringPreparation, String password, byte[] salt, int iteration);
}
