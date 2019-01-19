/**
 * Copyright (c) 2013-2016, impossibl.com
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

import java.util.Locale;

public class Locales {

  /**
   * If the locale specifier matches a win32 type locale (e.g. "English_United States.1252") it is
   * mapped to a matching Java locale spec by manually looking through available locales and matching
   * it based on full language name and full country name.
   *
   * @param localeSpec Locale specifier to test and transform
   * @return Java compatible locale specifier
   */
  public static String getJavaCompatibleLocale(String localeSpec) {
    String[] parts = localeSpec.split("\\.");
    if (parts.length == 1)
      return localeSpec;
    parts = parts[0].split("_");
    if (parts.length != 2) {
      return localeSpec;
    }
    // Manual search for locale...
    for (Locale locale : Locale.getAvailableLocales()) {
      if (locale.getDisplayLanguage().equals(parts[0]) && locale.getDisplayCountry().equals(parts[1])) {
        return locale.toString();
      }
    }
    return localeSpec;
  }

}
