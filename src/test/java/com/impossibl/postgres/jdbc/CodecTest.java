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
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.data.Interval;
import com.impossibl.postgres.data.Range;
import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.datetime.instants.AmbiguousInstant;
import com.impossibl.postgres.datetime.instants.Instant;
import com.impossibl.postgres.datetime.instants.PreciseInstant;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.PrimitiveType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;
import com.impossibl.postgres.utils.NullChannelBuffer;
import com.impossibl.postgres.utils.guava.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.TimeZone;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;



public class CodecTest {

  public Object[][] binaryData;

  PGConnection conn;

  @Before
  public void setUp() throws Exception {

    conn = (PGConnection) TestUtil.openDB();

    TestUtil.createType(conn, "teststruct" , "str text, str2 text, id uuid, num float");

    CompositeType teststructType = (CompositeType) conn.getRegistry().loadType("teststruct");
    assertNotNull(teststructType);

    binaryData = new Object[][] {
      {"int4[]", new Integer[] {1, 2, 3}},
      {"bit", new BitSet(42)},
      {"bool", true},
      {"bytea", new ByteArrayInputStream(new byte[5])},
      {"date", new AmbiguousInstant(Instant.Type.Date, 0)},
      {"float4", 1.23f},
      {"float8", 2.34d},
      {"int2", (short)234},
      {"int4", 234},
      {"int8", (long)234},
      {"interval", new Interval(1, 2, 3)},
      {"money", new BigDecimal("2342.00")},
      {"name", "hi"},
      {"numeric", new BigDecimal("2342.00")},
      {"oid", 132},
      {"int4range", Range.create(0, 5)},
      {"teststruct", new Record(teststructType, new Object[] {"hi", "hello", UUID.randomUUID(), 2.d})},
      {"text", "hi"},
      {"timestamp", new AmbiguousInstant(Instant.Type.Timestamp, 123)},
      {"timestamptz", new PreciseInstant(Instant.Type.Timestamp, 123, TimeZone.getTimeZone("UTC"))},
      {"time", new AmbiguousInstant(Instant.Type.Time, 123)},
      {"timetz", new PreciseInstant(Instant.Type.Time, 123, TimeZone.getTimeZone("GMT+00:00"))},
      {"uuid", UUID.randomUUID()},
      {"xml", "<hi></hi>".getBytes()}
    };
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropType(conn, "teststruct");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testBinaryCodecs() throws IOException {

    for (Object[] data : binaryData) {

      String typeName = (String) data[0];
      Object val = data[1];

      Type type = conn.getRegistry().loadType(typeName);
      Codec codec = type.getBinaryCodec();

      if (codec.encoder.getOutputPrimitiveType() == PrimitiveType.Unknown)
        continue;

      if (val instanceof InputStream) {
        ((InputStream) val).mark(Integer.MAX_VALUE);
        assertStreamEquals(typeName + " (bin): ", (InputStream) val, (InputStream) inOut(type, codec, val));
      }
      else if (val instanceof byte[]) {
        assertArrayEquals(typeName + " (bin): ", (byte[]) val, (byte[]) inOut(type, codec, val));
      }
      else if (val instanceof Object[]) {
        assertArrayEquals(typeName + " (bin): ", (Object[]) val, (Object[]) inOut(type, codec, val));
      }
      else {
        assertEquals(typeName + " (bin): ", val, inOut(type, codec, val));
      }
    }

  }

  @Test
  public void testBinaryEncoderLength() throws IOException {

    for (Object[] data : binaryData) {

      String typeName = (String) data[0];
      Object val = data[1];

      Type type = conn.getRegistry().loadType(typeName);

      compareLengths(typeName + " (bin): ", val, type, type.getBinaryCodec());
    }

  }

  private Object inOut(Type type, Codec codec, Object value) throws IOException {
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    codec.encoder.encode(type, buffer, value, conn);
    return codec.decoder.decode(type, buffer, conn);
  }

  private void compareLengths(String typeName, Object val, Type type, Codec codec) throws IOException {

    // Compute length with encoder
    int length = codec.encoder.length(type, val, conn);

    // Compute length using null channel buffer
    NullChannelBuffer lengthComputer = new NullChannelBuffer();
    codec.encoder.encode(type, lengthComputer, val, conn);

    assertEquals(typeName + " computes length incorrectly", lengthComputer.readableBytes(), length);
  }

  private void assertStreamEquals(String message, InputStream expected, InputStream actual) throws IOException {
    expected.reset();
    assertArrayEquals(message, ByteStreams.toByteArray(expected), ByteStreams.toByteArray(actual));
  }

}
