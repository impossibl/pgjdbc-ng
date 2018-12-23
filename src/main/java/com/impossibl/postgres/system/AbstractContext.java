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

import java.util.function.Function;

public abstract class AbstractContext implements Context {
  @Override
  public Object getSetting(String name) {
    return System.getProperty(name);
  }

  @Override
  public <T> T getSetting(String name, Class<T> type) {
    return type.cast(getSetting(name));
  }

  public <T> T getSetting(String name, Function<Object, T> converter) {
    return converter.apply(getSetting(name));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getSetting(String name, T defaultValue) {
    Object val = getSetting(name);
    if (val == null)
      return defaultValue;
    if ((defaultValue.getClass() == int.class || defaultValue.getClass() == Integer.class) && val instanceof String) {
      return (T) Integer.valueOf((String) val);
    }
    if ((defaultValue.getClass() == long.class || defaultValue.getClass() == Long.class) && val instanceof String) {
      return (T) Long.valueOf((String) val);
    }
    if ((defaultValue.getClass() == boolean.class || defaultValue.getClass() == Boolean.class) && val instanceof String) {
      return (T) Boolean.valueOf((String) val);
    }
    return (T) defaultValue.getClass().cast(val);
  }

}
