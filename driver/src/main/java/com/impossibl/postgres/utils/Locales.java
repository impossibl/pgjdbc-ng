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

import com.impossibl.postgres.utils.guava.Strings;

import java.util.Locale;

public class Locales {

  /**
   * Parse the given {@code String} value into a {@link Locale}, accepting
   * the {@link Locale#toString} format as well as BCP 47 language tags and
   * Win32 verbose locale names. If the locale may include an encoding
   * (e.g. UTF-8) or codepage (e.g. 1252) following a '.', it will be stripped.
   * "C" &amp; "POSIX" locales are always mapped to {@link Locale#ROOT}
   * @param localeValue the locale value: following either {@code Locale's}
   * {@code toString()} format ("en", "en_UK", etc), or BCP 47 (e.g. "en-UK")
   * as specified by {@link Locale#forLanguageTag} on Java 7+, or Win32 verbose
   * locale names (e.g. "English_United States"). Optionally including an ignored
   * encoding or Win32 codepage.
   * @return a corresponding {@code Locale} instance, or {@code null} if none
   * @throws IllegalArgumentException in case of an invalid locale specification
   */
  public static Locale parseLocale(String localeValue) {
    // Strip encoding/codepage
    localeValue = localeValue.split("\\.", 2)[0];
    if (localeValue.startsWith("Norwegian Bokm")) {
      localeValue = "Norwegian Bokmal";
    }

    switch (localeValue.toUpperCase(Locale.ROOT)) {
      case "C":
      case "POSIX":
        return Locale.ROOT;
    }

    String[] tokens = tokenizeLocaleSource(localeValue);
    if (tokens.length == 1) {
      validateLocalePart(localeValue);
      Locale resolved = Locale.forLanguageTag(localeValue);
      if (resolved.getLanguage().length() > 0) {
        return resolved;
      }
    }

    Locale locale = parseLocaleTokens(localeValue, tokens);

    if (locale == null) {
      // Manually search verbose names (handles win32 names)
      String language = tokens.length > 0 ? tokens[0] : "";
      String country = tokens.length > 1 ? tokens[1] : "";
      for (Locale availLocale : Locale.getAvailableLocales()) {
        if (availLocale.getDisplayLanguage(Locale.ENGLISH).equals(language) &&
            availLocale.getDisplayCountry(Locale.ENGLISH).equals(country)) {
          locale = availLocale;
          break;
        }
      }
    }

    return locale;
  }

  private static String[] tokenizeLocaleSource(String localeSource) {
    return localeSource.split("_");
  }

  private static Locale parseLocaleTokens(String localeString, String[] tokens) {
    String language = (tokens.length > 0 ? tokens[0] : "");
    String country = (tokens.length > 1 ? tokens[1] : "");
    validateLocalePart(language);
    validateLocalePart(country);

    String variant = "";
    if (tokens.length > 2) {
      // There is definitely a variant, and it is everything after the country
      // code sans the separator between the country code and the variant.
      int endIndexOfCountryCode = localeString.indexOf(country, language.length()) + country.length();
      // Strip off any leading '_' and whitespace, what's left is the variant.
      variant = trimLeadingWhitespace(localeString.substring(endIndexOfCountryCode));
      if (variant.startsWith("_")) {
        variant = trimLeadingCharacter(variant, '_');
      }
    }

    if (variant.isEmpty() && country.startsWith("#")) {
      variant = country;
      country = "";
    }

    if (!isISOLanguage(language) || !isISOCountry(country)) {
      return null;
    }

    return (language.length() > 0 ? new Locale(language, country, variant) : null);
  }

  private static boolean isISOCountry(String code) {
    if (code.length() > 2) {
      return false;
    }
    code = code.toUpperCase();
    for (String isoCode : Locale.getISOCountries()) {
      if (code.equals(isoCode)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isISOLanguage(String code) {
    if (code.length() > 2) {
      return false;
    }
    code = code.toLowerCase();
    for (String isoCode : Locale.getISOLanguages()) {
      if (code.equals(isoCode)) {
        return true;
      }
    }
    return false;
  }

  private static void validateLocalePart(String localePart) {
    for (int i = 0; i < localePart.length(); i++) {
      char ch = localePart.charAt(i);
      if (ch != ' ' && ch != '_' && ch != '-' && ch != '#' && ch != '(' && ch != ')' && !Character.isLetterOrDigit(ch)) {
        throw new IllegalArgumentException(
            "Locale part \"" + localePart + "\" contains invalid characters");
      }
    }
  }

  /**
   * Trim leading whitespace from the given {@code String}.
   * @param str the {@code String} to check
   * @return the trimmed {@code String}
   * @see java.lang.Character#isWhitespace
   */
  private static String trimLeadingWhitespace(String str) {
    if (Strings.isNullOrEmpty(str)) {
      return str;
    }

    StringBuilder sb = new StringBuilder(str);
    while (sb.length() > 0 && Character.isWhitespace(sb.charAt(0))) {
      sb.deleteCharAt(0);
    }
    return sb.toString();
  }

  /**
   * Trim all occurrences of the supplied leading character from the given {@code String}.
   * @param str the {@code String} to check
   * @param leadingCharacter the leading character to be trimmed
   * @return the trimmed {@code String}
   */
  private static String trimLeadingCharacter(String str, char leadingCharacter) {
    if (Strings.isNullOrEmpty(str)) {
      return str;
    }

    StringBuilder sb = new StringBuilder(str);
    while (sb.length() > 0 && sb.charAt(0) == leadingCharacter) {
      sb.deleteCharAt(0);
    }
    return sb.toString();
  }

}
