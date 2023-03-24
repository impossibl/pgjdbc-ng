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

import com.impossibl.postgres.jdbc.PGSQLSimpleException;

import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.security.AlgorithmParameters;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.SQLException;
import java.util.Collection;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.x500.X500Principal;



/**
 * A Key manager that only loads the keys, if necessary.
 *
 */
public class OnDemandKeyManager extends X509ExtendedKeyManager {

  private X509Certificate[] certificates = null;
  private PrivateKey key = null;
  private String certfile;
  private String keyfileName;
  private CallbackHandler cbh;
  private SSLFileReaderFactory readerFactory;
  private boolean defaultfile;
  private PGSQLSimpleException error = null;

  /**
   * Constructor. certfile and keyfile can be null, in that case no certificate
   * is presented to the server.
   *
   * @param certfile
   * @param keyfile
   * @param cbh
   * @param defaultfile
   */
  public OnDemandKeyManager(String certfile, String keyfile, CallbackHandler cbh, SSLFileReaderFactory readerFactory, boolean defaultfile) {
    this.certfile = certfile;
    this.keyfileName = keyfile;
    this.cbh = cbh;
    this.defaultfile = defaultfile;
    this.readerFactory = readerFactory;
  }

  /**
   * getCertificateChain and getPrivateKey cannot throw exeptions, therefore any
   * exception is stored in this.error and can be raised by this method
   *
   * @throws SQLException
   */
  public void throwKeyManagerException() throws SQLException {
    if (error != null)
      throw error;
  }

  @Override
  public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {

    if (certfile == null) {
      return null;
    }
    else {
      if (issuers == null || issuers.length == 0) {
        // Postgres 8.4 and earlier do not send the list of
        // accepted certificate authorities to the client. See BUG
        // #5468. We only hope, that our certificate will be accepted.
        return "user";
      }
      else {
        // Sending a wrong certificate makes the connection rejected, even,
        // if clientcert=0 in pg_hba.conf. therefore we only send our
        // certificate, if the issuer is listed in
        // issuers
        X509Certificate[] certchain = getCertificateChain("user");
        if (certchain == null) {
          return null;
        }
        else {
          X500Principal ourissuer = certchain[certchain.length - 1].getIssuerX500Principal();
          boolean found = false;
          for (int i = 0; i < issuers.length; i++) {
            if (ourissuer.equals(issuers[i])) {
              found = true;
            }
          }
          return found ? "user" : null;
        }
      }
    }
  }

  @Override
  public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
    return null;
  }

  @Override
  public X509Certificate[] getCertificateChain(String alias) {

    // If certfile is null, we do not load the certificates
    if (certificates == null && certfile != null) {

      // The certificate must be loaded
      CertificateFactory cf;
      try {
        cf = CertificateFactory.getInstance("X.509");
      }
      catch (CertificateException ex) {
        error = new PGSQLSimpleException("Could not find a java cryptographic algorithm: X.509 CertificateFactory not available");
        return null;
      }

      Collection<? extends Certificate> certs;
      try {
        certs = cf.generateCertificates(this.readerFactory.create(certfile));
      }
      catch (FileNotFoundException ioex) {
        if (!defaultfile) {
          // It is not an error if there is no file at the default location
          error = new PGSQLSimpleException("Could not open SSL certificate file " + certfile, ioex);
        }
        return null;
      }
      catch (CertificateException gsex) {
        error = new PGSQLSimpleException("Loading the SSL certificate " + certfile + " into a KeyManager failed", gsex);
        return null;
      }

      certificates = certs.toArray(new X509Certificate[certs.size()]);
    }

    return certificates;
  }

  @Override
  public String[] getClientAliases(String keyType, Principal[] issuers) {
    String alias = chooseClientAlias(new String[] {keyType}, issuers, (Socket) null);
    return alias == null ? new String[] {} : new String[] {alias};
  }

  @Override
  public PrivateKey getPrivateKey(String alias) {

    try {

      // If keyFileName is null, we do not load the key
      if (key == null && keyfileName != null) {

        // The private key must be loaded

        if (certificates == null) {
          // We need the certificate for the algorithm
          if (getCertificateChain("user") == null) {
            return null;
          }

        }

        byte[] keydata;

        try (InputStream fl = this.readerFactory.create(keyfileName)) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          byte[] buffer = new byte[1024];
          int length;
          while ((length = fl.read(buffer)) != -1) {
            result.write(buffer, 0, length);
          }
          keydata = baos.toByteArray();
        }
        catch (FileNotFoundException ex) {
          if (!defaultfile) {
            // It is not an error if there is no file at the default location
            throw ex;
          }
          return null;
        }

        KeyFactory keyFactory = KeyFactory.getInstance(certificates[0].getPublicKey().getAlgorithm());
        try {
          KeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keydata);
          key = keyFactory.generatePrivate(pkcs8KeySpec);
        }
        catch (InvalidKeySpecException ex) {

          // The key might be password protected
          EncryptedPrivateKeyInfo ePKInfo = new EncryptedPrivateKeyInfo(keydata);
          Cipher cipher;
          try {
            cipher = Cipher.getInstance(ePKInfo.getAlgName());
          }
          catch (NoSuchPaddingException npex) {
            throw new NoSuchAlgorithmException(npex.getMessage(), npex);
          }

          // We call back for the password
          PasswordCallback pwdcb = new PasswordCallback("Enter SSL password:", false);
          try {
            cbh.handle(new Callback[] {pwdcb});
          }
          catch (UnsupportedCallbackException ucex) {
            error = new PGSQLSimpleException("Could not read password for SSL key file, console is not available", ucex);
            return null;
          }

          try {

            PBEKeySpec pbeKeySpec = new PBEKeySpec(pwdcb.getPassword());

            // Now create the Key from the PBEKeySpec
            SecretKeyFactory skFac = SecretKeyFactory.getInstance(ePKInfo.getAlgName());
            Key pbeKey = skFac.generateSecret(pbeKeySpec);

            // Extract the iteration count and the salt
            AlgorithmParameters algParams = ePKInfo.getAlgParameters();
            cipher.init(Cipher.DECRYPT_MODE, pbeKey, algParams);

            // Decrypt the encryped private key into a PKCS8EncodedKeySpec
            KeySpec pkcs8KeySpec = ePKInfo.getKeySpec(cipher);
            key = keyFactory.generatePrivate(pkcs8KeySpec);
          }
          catch (GeneralSecurityException ikex) {
            error = new PGSQLSimpleException("Could not decrypt SSL key file " + keyfileName, ikex);
            return null;
          }
        }
      }
    }
    catch (IOException ioex) {
      error = new PGSQLSimpleException("Could not read SSL key file " + keyfileName, ioex);
    }
    catch (NoSuchAlgorithmException ex) {
      error = new PGSQLSimpleException("Could not find a java cryptographic algorithm: " + ex.getMessage(), ex);
      return null;
    }

    return key;
  }

  @Override
  public String[] getServerAliases(String keyType, Principal[] issuers) {
    return new String[] {};
  }

  @Override
  public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
    return chooseClientAlias(keyType, issuers, null);
  }

}
