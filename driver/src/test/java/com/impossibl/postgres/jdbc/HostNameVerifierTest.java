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
package com.impossibl.postgres.jdbc;

import static com.impossibl.postgres.protocol.v30.HostNameVerifier.verifyHostName;

import java.io.FileInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLPeerUnverifiedException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HostNameVerifierTest {

  static CertificateFactory factory;

  @BeforeAll
  public static void loadTestCert() throws Exception {
    factory = CertificateFactory.getInstance("X509");
  }

  @Test
  public void testMatchAgainstMultipleCommonNames() throws Exception {
    FileInputStream certStream = new FileInputStream("src/test/resources/multiple-cns.crt");
    X509Certificate cert = (X509Certificate) factory.generateCertificate(certStream);

    assertDoesNotThrow(() -> verifyHostName("www.widget-works.com", cert));
    assertThrows(
        SSLPeerUnverifiedException.class,
        () -> verifyHostName("widget-works.com", cert)
    );
  }

  @Test
  public void testMatchAgainstMultipleCommonNamesInReverseOrder() throws Exception {
    FileInputStream certStream = new FileInputStream("src/test/resources/multiple-cns-rev.crt");
    X509Certificate cert = (X509Certificate) factory.generateCertificate(certStream);

    assertDoesNotThrow(() -> verifyHostName("www.widget-works.com", cert));
    assertThrows(
        SSLPeerUnverifiedException.class,
        () -> verifyHostName("widget-works.com", cert)
    );
  }

  @Test
  public void testMatchAgainstSAN() throws Exception {
    FileInputStream certStream = new FileInputStream("src/test/resources/multiple-sans.crt");
    X509Certificate cert = (X509Certificate) factory.generateCertificate(certStream);

    assertDoesNotThrow(() -> verifyHostName("widget-works.com", cert));
    assertDoesNotThrow(() -> verifyHostName("app.widget-works.net", cert));
    assertDoesNotThrow(() -> verifyHostName("api.widget-works.net", cert));
    assertDoesNotThrow(() -> verifyHostName("app.env.widget-works.io", cert));
    assertDoesNotThrow(() -> verifyHostName("api.env.widget-works.io", cert));
  }

  @Test
  public void testNoMatchAgainstMultiSegmentWildcard() throws Exception {
    FileInputStream certStream = new FileInputStream("src/test/resources/multiple-sans.crt");
    X509Certificate cert = (X509Certificate) factory.generateCertificate(certStream);

    assertThrows(
        SSLPeerUnverifiedException.class,
        () -> verifyHostName("a.b.widget-works.net", cert)
    );
    assertThrows(
        SSLPeerUnverifiedException.class,
        () -> verifyHostName("a.b.env.widget-works.io", cert)
    );
  }

  @Test
  public void testMatchAgainstCommonNameWhenNoDnsSan() throws Exception {
    FileInputStream certStream = new FileInputStream("src/test/resources/ip-san-only.crt");
    X509Certificate cert = (X509Certificate) factory.generateCertificate(certStream);

    assertDoesNotThrow(() -> verifyHostName("www.widget-works.com", cert));
  }

  @Test
  public void testIPAddressesMatch() throws Exception {
    FileInputStream certStream = new FileInputStream("src/test/resources/ip-san-only.crt");
    X509Certificate cert = (X509Certificate) factory.generateCertificate(certStream);

    assertDoesNotThrow(() -> verifyHostName("127.0.0.1", cert));
  }

}
