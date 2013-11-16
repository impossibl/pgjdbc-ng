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

import com.impossibl.postgres.data.ACLItem;
import com.impossibl.postgres.data.Inet;
import com.impossibl.postgres.data.Interval;
import com.impossibl.postgres.data.Range;
import com.impossibl.postgres.data.Record;
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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;



@RunWith(Parameterized.class)
public class CodecTest {

  PGConnectionImpl conn;
  boolean binary, text;
  String typeName;
  String typeCastName;
  Object value;

  public CodecTest(String tag, boolean binary, boolean text, String typeName, Object value) {
    this.binary = binary;
    this.text = text;
    this.typeName = typeName;
    this.typeCastName = typeName;
    this.value = value;
  }

  @Before
  public void setUp() throws Exception {

    conn = (PGConnectionImpl) TestUtil.openDB();

    TestUtil.createType(conn, "teststruct" , "str text, str2 text, id uuid, num float");
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropType(conn, "teststruct");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testBinaryCodecs() throws IOException, SQLException {

    if (!binary)
      return;

    Type type = conn.getRegistry().loadType(typeName);
    if (type == null) {
      System.out.println("Skipping " + typeName + " (bin)");
      return;
    }

    Codec codec = type.getBinaryCodec();

    if (value instanceof Maker) {
      value = ((Maker) value).make(conn);
    }

    Class<?> targetType = type.getBinaryCodec().encoder.getInputType();

    value = SQLTypeUtils.coerce(value, type, targetType, conn.getTypeMap(), TimeZone.getDefault(), conn);

    if (codec.encoder.getOutputPrimitiveType() == PrimitiveType.Unknown) {
      System.out.println("Skipping " + typeName + " (bin)");
      return;
    }

    test(inOut(type, codec, value));
  }

  @Test
  public void testTextCodecs() throws IOException, SQLException {

    if (!text)
      return;

    Type type = conn.getRegistry().loadType(typeName);
    if (type == null) {
      System.out.println("Skipping " + typeName + " (txt)");
      return;
    }

    Codec codec = type.getTextCodec();

    if (value instanceof Maker) {
      value = ((Maker) value).make(conn);
    }

    Class<?> targetType = type.getTextCodec().encoder.getInputType();

    value = SQLTypeUtils.coerce(value, type, targetType, conn.getTypeMap(), TimeZone.getDefault(), conn);

    if (codec.encoder.getOutputPrimitiveType() == PrimitiveType.Unknown) {
      System.out.println("Skipping " + typeName + " (txt)");
      return;
    }

    test(inOut(type, codec, value));
  }

  @Test
  public void testBinaryEncoderLength() throws IOException, SQLException {

    if (!binary)
      return;

    Type type = conn.getRegistry().loadType(typeName);
    if (type == null) {
      System.out.println("Skipping " + typeName + " (binlen)");
      return;
    }

    if (value instanceof Maker) {
      value = ((Maker) value).make(conn);
    }

    Class<?> targetType = type.getBinaryCodec().encoder.getInputType();

    value = SQLTypeUtils.coerce(value, type, targetType, conn.getTypeMap(), TimeZone.getDefault(), conn);

    compareLengths(typeName + " (bin): ", value, type, type.getBinaryCodec());

  }

  @Test
  public void testSendReceive() throws SQLException, IOException {

    if (!binary)
      return;

    String typeCast = typeCasts.get(typeName);
    if (typeCast == null) {
      typeCast = typeName;
    }

    try (PreparedStatement stmt = conn.prepareStatement("SELECT ?::" + typeCast)) {

      if (value instanceof Maker) {
        value = ((Maker) value).make(conn);
      }

      stmt.setObject(1, value);

      try (ResultSet rs = stmt.executeQuery()) {

        rs.next();

        test(rs.getObject(1));

      }

    }

  }

  private Object inOut(Type type, Codec codec, Object value) throws IOException {
    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    codec.encoder.encode(type, buffer, value, conn);
    return codec.decoder.decode(type, null, null, buffer, conn);
  }

  public void test(Object actual) throws IOException {

    if (value instanceof InputStream) {
      ((InputStream) value).reset();
      assertStreamEquals(typeName + " (txt): ", (InputStream) value, (InputStream) actual);
    }
    else if (value instanceof byte[]) {
      assertArrayEquals(typeName + " (txt): ", (byte[]) value, (byte[]) actual);
    }
    else if (value instanceof Object[]) {
      assertArrayEquals(typeName + " (txt): ", (Object[]) value, (Object[]) actual);
    }
    else {
      assertEquals(typeName + " (txt): ", value, actual);
    }

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

  interface Maker {
    Object make(PGConnectionImpl conn);
  }

  @Parameters(name = "test-{3}-{0}")
  @SuppressWarnings("deprecation")
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][] {
      {"txt", false, true, "aclitem", new ACLItem("test", "rw", "root")},
      {"both", true,  true, "int4[]", new Integer[] {1, 2, 3}},
      {"both", true,  true, "bit", BitSet.valueOf(new byte[] {(byte) 0x7f})},
      {"both", true,  true, "varbit", BitSet.valueOf(new byte[] {(byte) 0xff, (byte) 0xff})},
      {"both", true,  true, "bool", true},
      {"both", true,  true, "bytea", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          return  new ByteArrayInputStream(new byte[5]);
        }

      } },
      {"both", true,  true, "date", new Date(2000, 1, 1)},
      {"both", true,  true, "float4", 1.23f},
      {"both", true,  true, "float8", 2.34d},
      {"both", true,  true, "int2", (short)234},
      {"both", true,  true, "int4", 234},
      {"both", true,  true, "int8", (long)234},
      {"both", true,  true, "interval", new Interval(1, 2, 3)},
      {"both", true,  true, "money", new BigDecimal("2342.00")},
      {"both", true,  true, "name", "hi"},
      {"both", true,  true, "numeric", new BigDecimal("2342.00")},
      {"both", true,  true, "oid", 132},
      {"both", true,  true, "int4range", Range.create(0, true, 5, false)},
      {"both", true,  true, "text", "hi"},
      {"both", true,  true, "timestamp", new Timestamp(2000, 1, 1, 0, 0, 0, 123000)},
      {"both", true,  true, "timestamptz", new Timestamp(2000, 1, 1, 0, 0, 0, 123000)},
      {"both", true,  true, "time", new Time(9, 30, 30)},
      {"both", true,  true, "timetz", new Time(9, 30, 30)},
      {"both", true,  true, "teststruct", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          return new Record((CompositeType) conn.getRegistry().loadType("teststruct"), new Object[] {"hi", "hello", UUID.randomUUID(), 2.d});
        }

      } },
      {"both", true,  true, "uuid", UUID.randomUUID()},
      {"bin",  true,  true, "xml", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          try {
            SQLXML sqlXML = conn.createSQLXML();
            sqlXML.setString("<xml></xml>");
            return sqlXML;
          }
          catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      } },
      {"both", true,  true, "macaddr", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          byte[] addr = new byte[6];
          new Random().nextBytes(addr);
          return addr;
        }

      } },
      {"both", true,  true, "hstore", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          Map<String, String> map = new HashMap<>();
          map.put("1", "one");
          map.put("2", "two");
          map.put("3", "three");
          return map;
        }

      } },
      {"both", true,  true, "inet", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          return new Inet("2001:4f8:3:ba:2e0:81ff:fe22:d1f1/10");
        }

      } },
    });
  }

  static Map<String, String> typeCasts;
  static {
    typeCasts = new HashMap<>();
    typeCasts.put("bit", "bit(7)");
  }

}
