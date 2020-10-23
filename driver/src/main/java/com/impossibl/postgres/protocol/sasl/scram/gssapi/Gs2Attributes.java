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


package com.impossibl.postgres.protocol.sasl.scram.gssapi;


import com.impossibl.postgres.protocol.sasl.scram.ScramAttributes;
import com.impossibl.postgres.protocol.sasl.scram.util.CharAttribute;


/**
 * Possible values of a GS2 Attribute.
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802#section-7">[RFC5802] Formal Syntax</a>
 */
public enum Gs2Attributes implements CharAttribute {
  /**
   * Channel binding attribute. Client doesn't support channel binding.
   */
  CLIENT_NOT(Gs2CbindFlag.DISABLED.getChar()),

  /**
   * Channel binding attribute. Client does support channel binding but thinks the server does not.
   */
  CLIENT_YES_SERVER_NOT(Gs2CbindFlag.NO_SERVER_SUPPORT.getChar()),

  /**
   * Channel binding attribute. Client requires channel binding. The selected channel binding follows "p=".
   */
  CHANNEL_BINDING_REQUIRED(Gs2CbindFlag.ENABLED.getChar()),

  /**
   * SCRAM attribute. This attribute specifies an authorization identity.
   */
  AUTHZID(ScramAttributes.AUTHZID.getChar());

  private final char flag;

  Gs2Attributes(char flag) {
    this.flag = flag;
  }

  @Override
  public char getChar() {
    return flag;
  }

  public static Gs2Attributes byChar(char c) {
    switch (c) {
      case 'n':
        return CLIENT_NOT;
      case 'y':
        return CLIENT_YES_SERVER_NOT;
      case 'p':
        return CHANNEL_BINDING_REQUIRED;
      case 'a':
        return AUTHZID;
    }

    throw new IllegalArgumentException("Invalid GS2Attribute character '" + c + "'");
  }

  public static Gs2Attributes byGS2CbindFlag(Gs2CbindFlag cbindFlag) {
    return byChar(cbindFlag.getChar());
  }
}
