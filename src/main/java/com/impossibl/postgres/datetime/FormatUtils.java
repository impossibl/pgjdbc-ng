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
package com.impossibl.postgres.datetime;

public class FormatUtils {

  static void checkOffset(String value, int offset, char expected) throws IndexOutOfBoundsException {
    if(offset < 0) {
      throw new IndexOutOfBoundsException("Not enough characters");
    }
    if(expected == '\0')
      return;
    char found = value.charAt(offset);
    if(found != expected) {
      throw new IndexOutOfBoundsException("Expected '" + expected + "' character but found '" + found + "'");
    }
  }

  static int parseInt(String value, int start, int[] res) {

    int i = start, end = value.length();
    int result = 0;
    int digit;
    if(i < end) {
      digit = Character.digit(value.charAt(i), 10);
      if(digit < 0) {
        return ~i;
      }
      i++;
      result = -digit;
    }
    while(i < end) {
      digit = Character.digit(value.charAt(i), 10);
      if(digit < 0) {
        break;
      }
      i++;
      result *= 10;
      result -= digit;
    }
    res[0] = -result;
    return i;
  }

}
