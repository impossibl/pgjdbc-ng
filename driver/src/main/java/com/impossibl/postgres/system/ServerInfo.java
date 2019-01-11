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
package com.impossibl.postgres.system;

import java.util.Objects;

public class ServerInfo {

  private Version version;
  private String encoding;
  private boolean integerDateTimes;

  public ServerInfo(Version version, String encoding, boolean integerDateTimes) {
    this.version = version;
    this.encoding = encoding;
    this.integerDateTimes = integerDateTimes;
  }

  public Version getVersion() {
    return version;
  }

  public String getEncoding() {
    return encoding;
  }

  public boolean hasIntegerDateTimes() {
    return integerDateTimes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ServerInfo that = (ServerInfo) o;
    return integerDateTimes == that.integerDateTimes &&
        version.equals(that.version) &&
        Objects.equals(encoding, that.encoding);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, encoding, integerDateTimes);
  }

  @Override
  public String toString() {
    return "ServerInfo{" +
        "version=" + version +
        ", encoding='" + encoding + '\'' +
        ", integerDateTimes=" + integerDateTimes +
        '}';
  }

}
