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

import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class StringTransforms {

  private static final Pattern CAPITALIZED_WORDS_PATTERN = Pattern.compile("[A-Z][^A-Z]*");

  /**
   * "un"-capitalizes a string in lowerCamelCase or UpperCamelCase
   * to all lowercase separated by dashes.
   *
   * @param val Value to un-capitalize
   * @return Un-capitalized version
   */
  public static String dashedFromCamelCase(String val) {
    return fromCamelCase(val, '-');
  }

  /**
   * "un"-capitalizes a string in lowerCamelCase or UpperCamelCase
   * to all lowercase separated by dots.
   *
   * @param val Value to un-capitalize
   * @return Un-capitalized version
   */
  public static String dottedFromCamelCase(String val) {
    return fromCamelCase(val, '.');
  }

  /**
   * "un"-capitalizes a string in lowerCamelCase or UpperCamelCase
   * to all lowercase separated by the provided {@code separator}.
   *
   * @param val Value to un-capitalize
   * @param separator Word separator
   * @return Un-capitalized version
   */
  public static String fromCamelCase(String val, char separator) {

    StringBuilder newVal = new StringBuilder();
    boolean first = true;

    Matcher matcher = CAPITALIZED_WORDS_PATTERN.matcher(val);
    while (matcher.find()) {
      String group = matcher.group(0);
      if (first) {
        // Handle "lowerCamelCase" first word
        if (matcher.start() != 0) {
          newVal.append(val, 0, matcher.start());
          newVal.append(separator);
        }
      }
      else {
        newVal.append(separator);
      }
      newVal.append(group.toLowerCase());
      first = false;
    }

    return newVal.toString();
  }

  /**
   * "un"-capitalizes a string in UPPER_SNAKE_CASE
   * to all lowercase separated by dashes.
   *
   * @param val Value to un-capitalize
   * @return Un-capitalized version
   */
  public static String dashedFromSnakeCase(String val) {
    return fromSnakeCase(val, '-');
  }

  /**
   * "un"-capitalizes a string in UPPER_SNAKE_CASE
   * to all lowercase separated by the provided {@code separator}.
   *
   * @param val Value to un-capitalize
   * @param separator Word separator
   * @return Un-capitalized version
   */
  public static String fromSnakeCase(String val, char separator) {
    return val.toLowerCase().replace('_', separator);
  }

  private static final Pattern WORD_BREAKS_PATTERN = Pattern.compile("[^.\\-_ ]+");

  /**
   * Attempt to transform the given value into an
   * "UPPER_SNAKE_CASE" version of the same string.
   *
   * Notably this replaces dots, dashes &amp; spaces
   * with underscores.
   *
   * @param val Value to transform
   * @return {@code UPPER_SNAKE_CASE} version of string.
   */
  public static String toUpperSnakeCase(String val) {

    StringBuilder newVal = new StringBuilder();
    boolean first = true;

    Matcher matcher = WORD_BREAKS_PATTERN.matcher(val);
    while (matcher.find()) {
      String group = matcher.group(0);
      if (!first) newVal.append("_");
      first = false;
      newVal.append(group.toUpperCase());
    }

    return newVal.toString();

  }

  /**
   * Attempt to transform the given value into an
   * "UpperCamelCase" version of the same string.
   *
   * Notably this erases dots, dashes, underscores
   * &amp; spaces as it counts them as word breaks.
   *
   * @param val Value to transform
   * @return {@code UpperCamelCase} version of string.
   */
  public static String toUpperCamelCase(String val) {
    return capitalizeGroups(val, true);
  }

  /**
   * Attempt to transform the given value into a
   * "lowerCamelCase" version of the same string.
   *
   * Notably this erases dots, dashes, underscores
   * &amp; spaces as it counts them as word breaks.
   *
   * @param val Value to transform
   * @return {@code lowerCamelCase} version of the string.
   */
  public static String toLowerCamelCase(String val) {
    return capitalizeGroups(val, false);
  }

  public static String capitalizeGroups(String val, boolean capitalizeFirstWord) {

    StringBuilder newVal = new StringBuilder();
    boolean first = !capitalizeFirstWord;

    Matcher matcher = WORD_BREAKS_PATTERN.matcher(val);
    while (matcher.find()) {
      String group = matcher.group(0);
      if (!first) {
        newVal
            .appendCodePoint(Character.toUpperCase(group.codePointAt(0)))
            .append(group.substring(1));
      }
      else {
        newVal.append(group);
      }
      first = false;
    }

    return newVal.toString();
  }

  public static String capitalize(String val) {
    return val.substring(0, 1).toUpperCase() + val.substring(1);
  }

}
