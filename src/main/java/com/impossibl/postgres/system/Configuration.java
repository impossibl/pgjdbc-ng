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

import static com.impossibl.postgres.utils.Types.boxType;
import static com.impossibl.postgres.utils.guava.Preconditions.checkNotNull;

import java.util.function.Function;

public interface Configuration {

  Object getSetting(String name);

  default  <T> T getSetting(String name, Class<T> type) {
    return getSetting(name, type, Function.identity());
  }

  default  <T> T getSetting(String name, Class<T> type, Function<String, String> transformer) {
    checkNotNull(type, "Type required, use getSetting(String) instead");
    Object value = getSetting(name);
    if (value == null) {
      return null;
    }
    return convert(value, type, transformer);
  }

  default  <T> T getSetting(String name, T defaultValue) {
    return getSetting(name, defaultValue, Function.identity());
  }

  default <T> T getSetting(String name, T defaultValue, Function<String, String> transformer) {
    checkNotNull(defaultValue, "Default value required, use getSetting(String) instead");
    Object value = getSetting(name);
    if (value == null) {
      return defaultValue;
    }
    @SuppressWarnings("unchecked")
    Class<T> type = (Class<T>) defaultValue.getClass();
    return convert(value, type, transformer);
  }

  @SuppressWarnings("unchecked")
  static <T> T convert(Object value, Class<T> type, Function<String, String> transformer) {
    type = boxType(type);
    if (type.isInstance(value)) {
      return type.cast(value);
    }
    if (value instanceof String) {
      String str = transformer.apply(value.toString());
      if (type == Boolean.class) {
        return type.cast(Boolean.valueOf(str));
      }
      if (type == Short.class) {
        return type.cast(Short.valueOf(str));
      }
      if (type == Integer.class) {
        return type.cast(Integer.valueOf(str));
      }
      if (type == Long.class) {
        return type.cast(Long.valueOf(str));
      }
      if (type == Float.class) {
        return type.cast(Float.valueOf(str));
      }
      if (type == Double.class) {
        return type.cast(Double.valueOf(str));
      }
      if (type.isEnum()) {
        try {
          return type.cast((Object) Enum.valueOf(type.asSubclass(Enum.class), str));
        }
        catch (IllegalArgumentException e) {
          // Try cased versions of the constant
          try {
            return type.cast((Object) Enum.valueOf(type.asSubclass(Enum.class), str.toUpperCase()));
          }
          catch (IllegalArgumentException e2) {
            return type.cast((Object) Enum.valueOf(type.asSubclass(Enum.class), str.toLowerCase()));
          }
        }
      }
    }
    throw new IllegalArgumentException("Value not convertible to requested type");
  }

}
