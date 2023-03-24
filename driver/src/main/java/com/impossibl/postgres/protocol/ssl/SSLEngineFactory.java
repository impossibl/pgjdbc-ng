/**
 * Copyright (c) 2013, impossibl.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of impossibl.com nor the names of its contributors may
 *    be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.impossibl.postgres.protocol.ssl;

import com.impossibl.postgres.system.Configuration;

import static com.impossibl.postgres.system.SystemSettings.SSL_CA_CRT_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_CRT_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_HOME_DIR;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_FILE;
import static com.impossibl.postgres.system.SystemSettings.SSL_KEY_PASSWORD_CALLBACK;
import static com.impossibl.postgres.system.SystemSettings.SSL_FILE_READER_FACTORY;

import java.io.InputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Iterator;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.callback.CallbackHandler;



public class SSLEngineFactory {


  private static final String TRUST_MANAGER_FACTORY_TYPE = "PKIX";
  private static final String SSL_PROTOCOL = "TLS";
  private static final String KEY_STORE_TYPE = "JKS";
  private static final String CERTIFICATE_FACTORY_TYPE = "X.509";


  public static SSLEngine create(SSLMode sslMode, Configuration config) throws IOException {

    /*
     * Load client's certificate and key file paths
     */

    boolean sslFileIsDefault = false;

    String sslCertFile = config.getSetting(SSL_CRT_FILE);
    sslFileIsDefault = sslFileIsDefault || SSL_CRT_FILE.getDefault().equals(sslCertFile);

    String sslKeyFile = config.getSetting(SSL_KEY_FILE);
    sslFileIsDefault = sslFileIsDefault || SSL_KEY_FILE.getDefault().equals(sslKeyFile);

    /*
     * Initialize Key Manager
     */

    @SuppressWarnings("unchecked")
    Class<? extends CallbackHandler> sslPasswordCallbackClass =
        (Class<? extends CallbackHandler>) config.getSetting(SSL_KEY_PASSWORD_CALLBACK);

    CallbackHandler sslPasswordCallback;
    try {
      sslPasswordCallback = sslPasswordCallbackClass.getConstructor().newInstance();
    }
    catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new IOException("Cannot instantiate provided password callback: " + sslPasswordCallbackClass.getName());
    }

    if (sslPasswordCallback instanceof ConfiguredCallbackHandler) {
      ((ConfiguredCallbackHandler) sslPasswordCallback).init(config);
    }

    @SuppressWarnings("unchecked")
    Class<? extends SSLFileReaderFactory> sslFileReaderFactoryClass =
        (Class<? extends SSLFileReaderFactory>) config.getSetting(SSL_FILE_READER_FACTORY);

    SSLFileReaderFactory sslFileReaderFactory;
    try {
      sslFileReaderFactory = sslFileReaderFactoryClass.getConstructor().newInstance();
    }
    catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new IOException("Cannot instantiate provided ssl file reader factory: " + sslFileReaderFactoryClass.getName());
    }

    KeyManager keyManager = new OnDemandKeyManager(sslCertFile, sslKeyFile, sslPasswordCallback, sslFileReaderFactory, sslFileIsDefault);

    /*
     * Initialize Trust Managers
     */

    TrustManager[] trustManagers;

    if (sslMode == SSLMode.VerifyCa || sslMode == SSLMode.VerifyFull) {

      TrustManagerFactory trustManagerFactory;
      try {
        trustManagerFactory = TrustManagerFactory.getInstance(TRUST_MANAGER_FACTORY_TYPE);
      }
      catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("trust manager not available", e);
      }

      KeyStore keyStore;
      try {
        keyStore = KeyStore.getInstance(KEY_STORE_TYPE);
      }
      catch (KeyStoreException e) {
        throw new RuntimeException("keystore not available", e);
      }

      /*
       * Load root certificates into a new key store (for Trust Manager)
       */

      String sslRootCertFile = config.getSetting(SSL_CA_CRT_FILE);
      if (SSL_CA_CRT_FILE.getDefault().equals(sslRootCertFile)) {
        sslRootCertFile = config.getSetting(SSL_HOME_DIR) + File.separator + sslRootCertFile;
      }

      try (InputStream sslRootCertInputStream = sslFileReaderFactory.create(sslRootCertFile)) {

        try {

          CertificateFactory certificateFactory = CertificateFactory.getInstance(CERTIFICATE_FACTORY_TYPE);

          Collection<? extends Certificate> certificates = certificateFactory.generateCertificates(sslRootCertInputStream);

          keyStore.load(null, null);

          Iterator<? extends Certificate> certificatesIter = certificates.iterator();
          for (int i = 0; certificatesIter.hasNext(); ++i) {
            keyStore.setCertificateEntry("cert" + i, certificatesIter.next());
          }

          trustManagerFactory.init(keyStore);
        }
        catch (GeneralSecurityException e) {
          throw new IOException("loading SSL root certificate failed", e);
        }

      }
      catch (FileNotFoundException e) {
        throw new IOException("cannot not open SSL root certificate file " + sslRootCertFile, e);
      }
      catch (IOException e1) {
        // Ignore...
      }

      trustManagers = trustManagerFactory.getTrustManagers();
    }
    else {

      trustManagers = new TrustManager[] {new NonValidatingTrustManager()};
    }

    /*
     * Initialize SSL context
     */

    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance(SSL_PROTOCOL);
    }
    catch (NoSuchAlgorithmException e) {
      throw new IOException("ssl context not available", e);
    }

    try {
      sslContext.init(new KeyManager[] {keyManager}, trustManagers, null);
    }
    catch (KeyManagementException e) {
      throw new IOException("ssl context initialization error", e);
    }

    SSLEngine sslEngine = sslContext.createSSLEngine();

    sslEngine.setUseClientMode(true);

    return sslEngine;
  }

}
