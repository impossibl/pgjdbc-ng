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

public class DecoratorContext implements Context {

  Context base;

  public DecoratorContext(Context base) {
    super();
    this.base = base;
  }

  @Override
  public Registry getRegistry() {
    return base.getRegistry();
  }

  @Override
  public TimeZone getTimeZone() {
    return base.getTimeZone();
  }

  @Override
  public Charset getCharset() {
    return base.getCharset();
  }

  @Override
  public KeyData getKeyData() {
    return base.getKeyData();
  }

  @Override
  public DateTimeFormat getDateFormatter() {
    return base.getDateFormatter();
  }

  @Override
  public DateTimeFormat getTimeFormatter() {
    return base.getTimeFormatter();
  }

  @Override
  public DateTimeFormat getTimestampFormatter() {
    return base.getTimestampFormatter();
  }

  @Override
  public DecimalFormat getDecimalFormatter() {
    return base.getDecimalFormatter();
  }

  @Override
  public DecimalFormat getCurrencyFormatter() {
    return base.getCurrencyFormatter();
  }

  @Override
  public Class<?> lookupInstanceType(Type type) {
    return base.lookupInstanceType(type);
  }

  @Override
  public void refreshType(int typeId) {
    base.refreshType(typeId);
  }

  @Override
  public void refreshRelationType(int relationId) {
    base.refreshRelationType(relationId);
  }

  @Override
  public Object getSetting(String name) {
    return base.getSetting(name);
  }

  @Override
  public <T> T getSetting(String name, Class<T> type) {
    return base.getSetting(name, type);
  }

  @Override
  public <T> T getSetting(String name, T defaultValue) {
    return base.getSetting(name, defaultValue);
  }

  @Override
  public boolean isSettingEnabled(String name) {
    return base.isSettingEnabled(name);
  }

  @Override
  public Protocol getProtocol() {
    return base.getProtocol();
  }

  @Override
  public void reportNotification(int processId, String channelName, String payload) {
    base.reportNotification(processId, channelName, payload);
  }

  @Override
  public Context unwrap() {
    return base.unwrap();
  }

}
