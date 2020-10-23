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


import com.impossibl.postgres.protocol.sasl.scram.ScramMechanism;
import com.impossibl.postgres.protocol.sasl.scram.ScramMechanisms;
import com.impossibl.postgres.protocol.sasl.scram.exception.ScramException;
import com.impossibl.postgres.protocol.sasl.scram.stringprep.StringPreparation;
import com.impossibl.postgres.protocol.sasl.scram.stringprep.StringPreparations;
import com.impossibl.postgres.protocol.sasl.scram.util.CryptoUtil;

import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;
import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.gt0;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;


/**
 * A factory to generate properly configured {@link ScramSession} instances. The {@link ScramSessionFactory} itself
 * must be instantiated using {@link ScramSessionFactory.Builder} to ensure a properly configured session factory.
 *
 * {@link ScramSessionFactory.Builder} is a declarative builder that allows consumers to provide only the required
 * information (e.g. server advertised mechanisms, channel-binding method, etc.) and ensures they receive a properly
 * configured factory with the strongest available authentication flow selected.
 *
 * Available builder methods for controlling authentication mechanism selection:
 * <ul>
 *     <li>
 *       {@link ScramSessionFactory.Builder#serverAdvertisedMechanisms} -
 *       The list of supported mechanisms advertised by the server.
 *     </li>
 *     <li>
 *       {@link ScramSessionFactory.Builder#channelBindMethod} -
 *       The selected/supported channel-bind method the client plans to use.
 *     </li>
 *     <li>
 *       {@link ScramSessionFactory.Builder#preferChannelBindingMechanism(boolean)} -
 *       Whether the selection process should favor selecting a mechanism that requires channel binding over a method
 *       that has a stronger algorithm. For example, if 'SCRAM-SHA1-PLUS' and 'SCRAM-SHA256' are the available methods
 *       should it favor the `SCRAM-SHA1-PLUS` because it requires channel binding.
 *     </li>
 * </ul>
 */
public class ScramSessionFactory {
  /**
   * Default nonce byte length
   */
  public static final int DEFAULT_NONCE_LENGTH = 24;

  private final ScramMechanism scramMechanism;
  private final String channelBindMethod;
  private final boolean serverSupportsChannelBinding;
  private final StringPreparation stringPreparation;
  private final int nonceLength;
  private final SecureRandom secureRandom;

  /**
   * Instantiates a {@link ScramSession} for the specified user with this factory's selected mechanism and
   * algorithmic features.
   *
   * @param user The username of the authentication exchange
   * @return The ScramSession instance
   */
  public ScramSession start(String user) {
    String nonce = CryptoUtil.nonce(nonceLength, secureRandom);
    return new ScramSession(
        scramMechanism,
        channelBindMethod, serverSupportsChannelBinding,
        stringPreparation,
        checkNotNull(user, "user"), nonce
    );
  }

  private ScramSessionFactory(ScramMechanism scramMechanism, String channelBindMethod, boolean serverSupportsChannelBinding,
                              StringPreparation stringPreparation, int nonceLength, SecureRandom secureRandom) {
    assert null != scramMechanism : "scramMechanism";
    assert null != stringPreparation : "stringPreparation";
    assert null != secureRandom : "secureRandom";

    this.scramMechanism = scramMechanism;
    this.channelBindMethod = channelBindMethod;
    this.serverSupportsChannelBinding = serverSupportsChannelBinding;
    this.stringPreparation = stringPreparation;
    this.nonceLength = nonceLength;
    this.secureRandom = secureRandom;
  }

  public static Builder builder() {
    try {
      return new Builder();
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static class Builder {
    private Collection<ScramMechanism> serverAdvertisedMechanisms;
    private String channelBindMethod;
    private boolean preferChannelBindingOverAlgorithmStrength;
    private StringPreparation stringPreparation;
    private int nonceLength;
    private SecureRandom secureRandom;

    private Builder() throws NoSuchAlgorithmException {
      this.serverAdvertisedMechanisms = Collections.emptyList();
      this.channelBindMethod = null;
      this.preferChannelBindingOverAlgorithmStrength = false;
      this.stringPreparation = StringPreparations.NO_PREPARATION;
      this.nonceLength = DEFAULT_NONCE_LENGTH;
      this.secureRandom = SecureRandom.getInstanceStrong();
    }

    /**
     * Provide the list of mechanism names advertised by the server. The method will filter out
     * mechanisms unsupported by the current implementation.
     *
     * @param serverAdvertisedMechanisms Names of advertised mechanisms
     */
    public Builder serverAdvertisedMechanisms(Collection<String> serverAdvertisedMechanisms) {
      checkNotNull(serverAdvertisedMechanisms, "serverAdvertisedMechanisms");
      this.serverAdvertisedMechanisms =
          serverAdvertisedMechanisms.stream()
              .map(ScramMechanisms::byName)
              .filter(Objects::nonNull)
              .collect(toList());
      return this;
    }

    /**
     * Determine whether the selection process should favor selecting a mechanism that requires channel
     * binding over a method that has a stronger algorithm. For example, if 'SCRAM-SHA1-PLUS' and 'SCRAM-SHA256'
     * are the available methods should it favor the `SCRAM-SHA1-PLUS` because it requires channel binding.
     *
     * @param preferChannelBinding Should selection prefer a channel binding mechanism
     */
    public Builder preferChannelBindingMechanism(boolean preferChannelBinding) {
      this.preferChannelBindingOverAlgorithmStrength = preferChannelBinding;
      return this;
    }

    /**
     * The selected/supported channel-bind method the client plans to use. If the client does not support any
     * channel binding methods it can pass {@code null}.
     *
     * @param channelBindMethod Selected/supported channel-bind method.
     */
    public Builder channelBindMethod(String channelBindMethod) {
      this.channelBindMethod = channelBindMethod;
      return this;
    }

    /**
     * Optional call. The string preparation method that should be performed on the user's name and password. The
     * default is to use {@link StringPreparations#SASL_PREPARATION}.
     *
     * @param stringPreparation Selected string preparation method.
     */
    public Builder stringPreparation(StringPreparation stringPreparation) {
      this.stringPreparation = checkNotNull(stringPreparation, "stringPreparation");
      return this;
    }

    /**
     * Optional call. The length of randomly generated nonce that should be used. Only required if a specific
     * non-default nonce length is required otherwise an implementation defined default length is used.
     *
     * @param nonceLength Length of randomly generated nonce.
     */
    public Builder nonceLength(int nonceLength) {
      this.nonceLength = gt0(nonceLength, "nonceLength");
      return this;
    }

    /**
     * Optional call. Selects a non-default SecureRandom instance, based on the given algorithm and
     * optionally a specific security provider. This selected {@link SecureRandom} instance will be used to
     * generate secure random values (e.g. nonces). Algorithm and provider names are those supported by the
     * {@link SecureRandom} class.
     *
     * @param algorithm The name of the algorithm to use.
     * @param provider The name of the provider of SecureRandom. Might be null.
     * @return The same class
     * @throws IllegalArgumentException If algorithm is null, or either the algorithm or provider are not supported
     */
    public Builder secureRandomAlgorithmProvider(String algorithm, String provider) throws IllegalArgumentException {
      checkNotNull(algorithm, "algorithm");
      try {
        secureRandom = null == provider ?
            SecureRandom.getInstance(algorithm) :
            SecureRandom.getInstance(algorithm, provider);
      }
      catch (NoSuchAlgorithmException | NoSuchProviderException e) {
        throw new IllegalArgumentException("Invalid algorithm or provider", e);
      }

      return this;
    }

    public ScramSessionFactory build() throws ScramException {
      // If channel binding is supported, find best mechanisms supporting it
      Optional<ScramMechanism> selectedChannelBindingScramMechanism = Optional.empty();
      if (channelBindMethod != null) {
        selectedChannelBindingScramMechanism =
            serverAdvertisedMechanisms.stream()
                .filter(ScramMechanism::requiresChannelBinding)
                .max(comparingInt(ScramMechanism::algorithmKeyLength));
      }

      // Find best non binding mechanism as well
      Optional<ScramMechanism> selectedNonChannelBindingScramMechanism =
          serverAdvertisedMechanisms.stream()
              .filter(scramMechanism -> !scramMechanism.requiresChannelBinding())
              .max(comparingInt(ScramMechanism::algorithmKeyLength));

      // Choose best mechanism based on availability and preference
      Optional<ScramMechanism> selectedScramMechanism;
      if (selectedChannelBindingScramMechanism.isPresent() && selectedNonChannelBindingScramMechanism.isPresent()) {
        if (preferChannelBindingOverAlgorithmStrength) {
          // Choose the channel binding mechanism
          selectedScramMechanism = selectedChannelBindingScramMechanism;
        }
        else {
          // Choose based on key length
          selectedScramMechanism =
              Stream.of(selectedChannelBindingScramMechanism.get(), selectedNonChannelBindingScramMechanism.get())
                  .max(comparingInt(ScramMechanism::algorithmKeyLength));
        }
      }
      else if (selectedChannelBindingScramMechanism.isPresent()) {
        selectedScramMechanism = selectedChannelBindingScramMechanism;
      }
      else {
        selectedScramMechanism = selectedNonChannelBindingScramMechanism;
      }

      if (!selectedScramMechanism.isPresent()) {
        String algorithmNames = serverAdvertisedMechanisms.stream().map(ScramMechanism::getName).collect(joining());
        throw new ScramException(format("Unable to negotiate supported mechanism (advertised %s)", algorithmNames));
      }

      return new ScramSessionFactory(
          selectedScramMechanism.get(), channelBindMethod,
          selectedChannelBindingScramMechanism.isPresent(), stringPreparation,
          nonceLength, secureRandom
      );
    }

  }

}
