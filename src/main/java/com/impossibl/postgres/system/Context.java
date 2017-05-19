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

import com.impossibl.postgres.datetime.DateTimeFormat;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.TimeZone;

public interface Context {

  class KeyData {
    private int processId;
    private int secretKey;

    KeyData(int processId, int secretKey) {
      this.processId = processId;
      this.secretKey = secretKey;
    }

    public int getProcessId() {
      return processId;
    }

    public int getSecretKey() {
      return secretKey;
    }
  }


  Registry getRegistry();

  TimeZone getTimeZone();

  Charset getCharset();

  KeyData getKeyData();

  DecimalFormat getDecimalFormatter();
  DecimalFormat getCurrencyFormatter();
  DateTimeFormat getDateFormatter();
  DateTimeFormat getTimeFormatter();
  DateTimeFormat getTimestampFormatter();

  Class<?> lookupInstanceType(Type type);

  void refreshType(int typeId);
  void refreshRelationType(int relationId);

  Object getSetting(String name);
  <T> T getSetting(String name, Class<T> type);
  <T> T getSetting(String name, T defaultValue);
  boolean isSettingEnabled(String name);

  Protocol getProtocol();

  void reportNotification(int processId, String channelName, String payload);

  Context unwrap();

}
