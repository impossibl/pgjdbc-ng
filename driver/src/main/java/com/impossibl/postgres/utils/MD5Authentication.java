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
package com.impossibl.postgres.utils;

import static com.impossibl.postgres.system.procs.Bytes.encodeHex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MD5Authentication {

  /**
   * Generates an authentication token compatible with PostgreSQL's
   * MD5 scheme
   *
   * @param password Password to generate token from
   * @param user Username to generate token from
   * @param salt Salt to generate token from
   * @return MD5 authentication token
   */
  public static String encode(String password, String user, byte[] salt) {

    byte[] tempDigest, passDigest;

    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    md.update(password.getBytes(UTF_8));
    md.update(user.getBytes(UTF_8));
    tempDigest = md.digest();

    byte[] hexDigest = encodeHex(tempDigest).toLowerCase().getBytes(US_ASCII);

    md.update(hexDigest);
    md.update(salt);
    passDigest = md.digest();

    return "md5" + encodeHex(passDigest).toLowerCase();
  }

}
