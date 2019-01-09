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
package com.impossibl.postgres.system.procs;

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.ConversionException;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.Joiner;

import java.io.IOException;
import java.text.ParseException;


public class Int2Vectors extends SimpleProcProvider {

  public Int2Vectors() {
    super(new TxtEncoder(), new TxtDecoder(), new Arrays.BinEncoder(), new Arrays.BinDecoder(), "int2vector");
  }

  static class TxtDecoder extends BaseTextDecoder {

    @Override
    public Class<?> getDefaultClass() {
      return Short[].class;
    }

    @Override
    protected Object decodeValue(Context context, Type type, Short typeLength, Integer typeModifier, CharSequence buffer, Class<?> targetClass, Object targetContext) throws IOException, ParseException {

      int length = buffer.length();

      Short[] values = null;

      if (length != 0) {
        String[] items = buffer.toString().split(" ");
        values = new Short[items.length];
        for (int c = 0; c < items.length; ++c) {
          values[c] = Short.parseShort(items[c]);
        }
      }

      return values;
    }

  }

  static class TxtEncoder extends BaseTextEncoder {

    @Override
    protected void encodeValue(Context context, Type type, Object value, Object sourceContext, StringBuilder buffer) throws IOException {

      String[] items;
      if (value instanceof short[]) {
        short[] values = (short[]) value;
        items = new String[values.length];
        for (int c = 0; c < values.length; ++c) {
          items[c] = Short.toString(values[c]);
        }
      }
      else if (value instanceof Short[]) {
        Short[] values = (Short[]) value;
        items = new String[values.length];
        for (int c = 0; c < values.length; ++c) {
          items[c] = Short.toString(values[c]);
        }
      }
      else {
        throw new ConversionException(value.getClass(), type);
      }

      Joiner.on(' ').appendTo(buffer, items);

    }

  }

}
