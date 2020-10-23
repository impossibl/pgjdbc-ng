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


package com.impossibl.postgres.protocol.sasl.scram.util;


import static com.impossibl.postgres.protocol.sasl.scram.util.Preconditions.checkNotNull;

import java.util.Arrays;


/**
 * Helper class to generate Comma Separated Values of {@link StringWritable}s
 */
public class StringWritableCsv {
  private static void writeStringWritableToStringBuffer(StringWritable value, StringBuffer sb) {
    if (null != value) {
      value.writeTo(sb);
    }
  }

  /**
   * Write a sequence of {@link StringWritableCsv}s to a StringBuffer.
   * Null {@link StringWritable}s are not printed, but separator is still used.
   * Separator is a comma (',')
   * @param sb The sb to write to
   * @param values Zero or more attribute-value pairs to write
   * @return The same sb, with data filled in (if any)
   * @throws IllegalArgumentException If sb is null
   */
  public static StringBuffer writeTo(StringBuffer sb, StringWritable... values) throws IllegalArgumentException {
    checkNotNull(sb, "sb");
    if (null == values || values.length == 0) {
      return sb;
    }

    writeStringWritableToStringBuffer(values[0], sb);
    int i = 1;
    while (i < values.length) {
      sb.append(',');
      writeStringWritableToStringBuffer(values[i], sb);
      i++;
    }

    return sb;
  }

  /**
   * Parse a String with a {@link StringWritableCsv} into its composing Strings
   * represented as Strings. No validation is performed on the individual attribute-values returned.
   * @param value The String with the set of attribute-values
   * @param n Number of entries to return (entries will be null of there were not enough). 0 means unlimited
   * @param offset How many entries to skip before start returning
   * @return An array of Strings which represent the individual attribute-values
   * @throws IllegalArgumentException If value is null or either n or offset are negative
   */
  public static String[] parseFrom(String value, int n, int offset) throws IllegalArgumentException {
    checkNotNull(value, "value");
    if (n < 0 || offset < 0) {
      throw new IllegalArgumentException("Limit and offset have to be >= 0");
    }

    if (value.isEmpty()) {
      return new String[0];
    }

    String[] split = value.split(",");
    if (split.length < offset) {
      throw new IllegalArgumentException("Not enough items for the given offset");
    }

    return Arrays.copyOfRange(
        split,
        offset,
        (n == 0 ? split.length : n) + offset
    );
  }

  /**
   * Parse a String with a {@link StringWritableCsv} into its composing Strings
   * represented as Strings. No validation is performed on the individual attribute-values returned.
   * Elements are returned starting from the first available attribute-value.
   * @param value The String with the set of attribute-values
   * @param n Number of entries to return (entries will be null of there were not enough). 0 means unlimited
   * @return An array of Strings which represent the individual attribute-values
   * @throws IllegalArgumentException If value is null or n is negative
   */
  public static String[] parseFrom(String value, int n) throws IllegalArgumentException {
    return parseFrom(value, n, 0);
  }

  /**
   * Parse a String with a {@link StringWritableCsv} into its composing Strings
   * represented as Strings. No validation is performed on the individual attribute-values returned.
   * All the available attribute-values will be returned.
   * @param value The String with the set of attribute-values
   * @return An array of Strings which represent the individual attribute-values
   * @throws IllegalArgumentException If value is null
   */
  public static String[] parseFrom(String value) throws IllegalArgumentException {
    return parseFrom(value, 0, 0);
  }
}
