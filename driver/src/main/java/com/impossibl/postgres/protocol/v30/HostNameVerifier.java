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
package com.impossibl.postgres.protocol.v30;

import java.net.IDN;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

public class HostNameVerifier {

  private static final Logger logger = Logger.getLogger(HostNameVerifier.class.getName());

  private static final int SAN_TYPE_DNS_NAME = 2;
  private static final int SAN_TYPE_IP_ADDRESS = 7;

  public static void verifyHostName(String hostName, SSLSession sslSession) throws SSLPeerUnverifiedException {

    // Load the server certificate from session
    X509Certificate[] peerCertificates = (X509Certificate[]) sslSession.getPeerCertificates();
    if (peerCertificates == null || peerCertificates.length == 0) {
      throw new SSLPeerUnverifiedException("No peer certificates for hostname verification");
    }

    verifyHostName(hostName, peerCertificates[0]);
  }

  public static void verifyHostName(String hostName, X509Certificate serverCertificate) throws SSLPeerUnverifiedException {

    // Canonicalize hostName for comparisons

    String canonicalHostName;
    if (hostName.startsWith("[") && hostName.endsWith("]")) {
      // IPv6 address like [2001:db8:0:1:1:1:1:1]
      canonicalHostName = hostName.substring(1, hostName.length() - 1);
    }
    else {
      try {
        canonicalHostName = IDN.toASCII(hostName);
      }
      catch (IllegalArgumentException e) {
        String failMessage = format("Hostname '%s' is invalid", hostName);
        throw new SSLPeerUnverifiedException(failMessage);
      }
    }

    logger.log(
        Level.FINE,
        "Translated hostname {0} to canonical hostname {1}",
        new Object[] {hostName, canonicalHostName}
    );


    // Check the certificate's subject alternate names against canonical hostname

    Collection<List<?>> subjectAltNameEntries = null;
    try {
      subjectAltNameEntries = serverCertificate.getSubjectAlternativeNames();
    }
    catch (CertificateParsingException e) {
      // Ignore and proceed as if no SANs exist
    }
    if (subjectAltNameEntries == null) {
      subjectAltNameEntries = Collections.emptyList();
    }

    boolean subjectAltNamesContainsDNSName = false;

    for (List<?> subjectAltNameEntry : subjectAltNameEntries) {
      if (subjectAltNameEntry.size() != 2) {
        continue;
      }

      Integer subjectAltNameType = (Integer) subjectAltNameEntry.get(0);
      if (subjectAltNameType != SAN_TYPE_IP_ADDRESS && subjectAltNameType != SAN_TYPE_DNS_NAME) {
        continue;
      }

      String subjectAltNameValue = (String) subjectAltNameEntry.get(1);
      if (subjectAltNameType == SAN_TYPE_IP_ADDRESS &&
          subjectAltNameValue != null &&
          subjectAltNameValue.contains("*")) {
        // Disallow wildcards for IP address
        continue;
      }

      subjectAltNamesContainsDNSName |= subjectAltNameType == SAN_TYPE_DNS_NAME;

      if (matchHostName(canonicalHostName, subjectAltNameValue)) {
        logger.log(Level.FINE, "Matched Subject Alternate Name to '{0}'", hostName);
        return;
      }
    }

    if (subjectAltNamesContainsDNSName) {
      // According to RFC2818, section 3.1
      logger.log(
          Level.SEVERE,
          "Aborting host name verification due to mismatching DNS Subject Alternate Name",
          hostName
      );

      String failMessage = format("Failed to match hostname '%s' against DNS Subject Alternate Name", hostName);
      throw new SSLPeerUnverifiedException(failMessage);
    }


    // Attempt to extract and match against a common name of the certificate's subject

    LdapName subject;
    try {
      subject = new LdapName(serverCertificate.getSubjectX500Principal().getName(X500Principal.RFC2253));
    }
    catch (InvalidNameException e) {
      throw new SSLPeerUnverifiedException("Certificate contains invalid subject");
    }

    // Extract all common name
    List<String> commonNames = new ArrayList<>(1);
    for (Rdn rdn : subject.getRdns()) {
      if ("CN".equals(rdn.getType())) {
        commonNames.add((String) rdn.getValue());
      }
    }

    if (commonNames.isEmpty()) {
      throw new SSLPeerUnverifiedException("Certificate subject missing common name");
    }
    else if (commonNames.size() > 1) {
      // According to RFC2818, section 3.1, the most specific common name must be used, sort accordingly.
      commonNames.sort(specificHostNameComparator);
    }

    String commonName = commonNames.get(commonNames.size() - 1);

    if (!matchHostName(canonicalHostName, commonName)) {
      String failMessage = format("Hostname '%s' could not be verified", hostName);
      throw new SSLPeerUnverifiedException(failMessage);
    }
  }

  public static boolean matchHostName(String hostName, String patternName) {
    if (hostName == null || patternName == null) {
      return false;
    }

    int lastWildCard = patternName.lastIndexOf('*');
    if (lastWildCard == -1) {
      return hostName.equalsIgnoreCase(patternName);
    }

    // The wildcard must be at the beginning of the pattern name, the pattern
    // name is a multi-segment pattern, and the hostname is at least as long
    // as the non-wildcard portion of the pattern
    if (lastWildCard > 0 ||
        patternName.indexOf('.') == -1 ||
        hostName.length() < patternName.length() - 1) {
      return false;
    }

    // Find start of comparison range in host name based on wildcard portion of
    // pattern name. Give the example (pattern-name=*.b.a) == (host-name=c.b.a)
    // we must compare only the "b.a".
    int nonWildcardComparisonOffset = hostName.length() - patternName.length() + 1;

    // Fail If wildcard covers more than one domain segment
    if (hostName.lastIndexOf('.', nonWildcardComparisonOffset - 1) >= 0) {
      return false;
    }

    return hostName.regionMatches(true, nonWildcardComparisonOffset, patternName, 1, patternName.length() - 1);
  }

  private static final Comparator<String> specificHostNameComparator = new Comparator<String>() {
    private int countChars(String value, char ch) {
      int count = 0;
      int pos = -1;
      while (true) {
        pos = value.indexOf(ch, pos + 1);
        if (pos == -1) {
          break;
        }
        count++;
      }
      return count;
    }

    @Override
    public int compare(String o1, String o2) {
      // More segments is more specific, e.g. d.c.b.a is more specific than c.b.a
      int d1 = countChars(o1, '.');
      int d2 = countChars(o2, '.');
      if (d1 != d2) {
        return d1 > d2 ? 1 : -1;
      }

      // Less wildcards is more specific, e.g. *.c.b.a is more specific than *.*.b.a
      int s1 = countChars(o1, '*');
      int s2 = countChars(o2, '*');
      if (s1 != s2) {
        return s1 < s2 ? 1 : -1;
      }

      // Finally, we choose the longer name
      int l1 = o1.length();
      int l2 = o2.length();
      if (l1 != l2) {
        return l1 > l2 ? 1 : -1;
      }

      return 0;
    }
  };

}
