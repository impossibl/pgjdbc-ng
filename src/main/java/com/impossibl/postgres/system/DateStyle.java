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
import com.impossibl.postgres.datetime.ISODateFormat;
import com.impossibl.postgres.datetime.ISOTimeFormat;
import com.impossibl.postgres.datetime.ISOTimestampFormat;

/**
 * Utility methods for handling PostgreSQL DateStyle parameters
 *
 * @author kdubb
 *
 */
public class DateStyle {

  /**
   * Parses a DateStyle string into its separate components
   *
   * @param value DateStyle to parse
   * @return Parsed DateStyle components
   */
  public static String[] parse(String value) {

    String[] parsed = value.split(",");
    if (parsed.length != 2)
      return null;

    parsed[0] = parsed[0].trim().toUpperCase();
    parsed[1] = parsed[1].trim().toUpperCase();

    return parsed;
  }

  /**
   * Creates a DateFormat for handling Dates from a parsed DateStyle string
   * 
   * @param dateStyle
   *          Parsed DateStyle
   * @return DateFormat for handling dates in the style specified in dateStyle
   */
  public static DateTimeFormat getDateFormatter(String[] dateStyle) {

    switch(dateStyle[0]) {
      case "ISO":
        return new ISODateFormat();

      case "POSTGRES":
      case "SQL":
      case "GERMAN":
      default:
        return null;
    }

  }

  /**
   * Creates a DateFormat for handling Times from a parsed DateStyle string
   * 
   * @param dateStyle
   *          Parsed DateStyle
   * @return DateFormat for handling times in the style specified in dateStyle
   */
  public static DateTimeFormat getTimeFormatter(String[] dateStyle) {

    switch(dateStyle[0]) {
      case "ISO":
        return new ISOTimeFormat();

      case "POSTGRES":
      case "SQL":
      case "GERMAN":
      default:
        return null;
    }

  }

  /**
   * Creates a DateFormat for handling Timestamps from a parsed DateStyle string
   * 
   * @param dateStyle
   *          Parsed DateStyle
   * @return DateFormat for handling timestamps in the style specified in
   *         dateStyle
   */
  public static DateTimeFormat getTimestampFormatter(String[] dateStyle) {

    switch(dateStyle[0]) {
      case "ISO":
        return new ISOTimestampFormat();

      case "POSTGRES":
      case "SQL":
      case "GERMAN":
      default:
        return null;
    }

  }

}
