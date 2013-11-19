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
import com.impossibl.postgres.data.CidrAddr;
import com.impossibl.postgres.data.InetAddr;
import com.impossibl.postgres.data.Interval;
import com.impossibl.postgres.data.Range;
import com.impossibl.postgres.data.Record;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Codec;
import com.impossibl.postgres.utils.NullChannelBuffer;
import com.impossibl.postgres.utils.guava.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  String typeName;
  Object value;

  public CodecTest(String typeName, Object value) {
    this.typeName = typeName;
    this.value = value;
  }

  @Before
  public void setUp() throws Exception {

    conn = (PGConnectionImpl) TestUtil.openDB();

    TestUtil.createType(conn, "teststruct" , "str text, str2 text, id uuid, num float");
    TestUtil.createType(conn, "teststructany", "anytype float");
  }

  @After
  public void tearDown() throws SQLException {
    TestUtil.dropType(conn, "teststruct");
    TestUtil.closeDB(conn);
  }

  @Test
  public void testBinaryCodecs() throws IOException, SQLException {

    makeValue();

    Type type = conn.getRegistry().loadType(typeName);
    if (type == null) {
      System.out.println("Skipping " + typeName + " (bin)");
      return;
    }

    if (!type.isParameterFormatSupported(Format.Binary) || !type.isResultFormatSupported(Format.Binary)) {
      System.out.println("Skipping " + typeName + " (bin)");
      return;
    }

    test(value, inOut(type, Format.Binary, value));
  }

  @Test
  public void testTextCodecs() throws IOException, SQLException {

    makeValue();

    Type type = conn.getRegistry().loadType(typeName);
    if (type == null) {
      System.out.println("Skipping " + typeName + " (txt)");
      return;
    }

    if (!type.isParameterFormatSupported(Format.Text) || !type.isResultFormatSupported(Format.Text)) {
      System.out.println("Skipping " + typeName + " (txt)");
      return;
    }

    test(value, inOut(type, Format.Text, value));
  }

  @Test
  public void testBinaryEncoderLength() throws IOException, SQLException {

    makeValue();

    Type type = conn.getRegistry().loadType(typeName);
    if (type == null) {
      System.out.println("Skipping " + typeName + " (binlen)");
      return;
    }

    if (!type.isParameterFormatSupported(Format.Binary) || !type.isResultFormatSupported(Format.Binary)) {
      System.out.println("Skipping " + typeName + " (binlen)");
      return;
    }

    compareLengths(coerceValue(type, Format.Binary, value), type, type.getBinaryCodec());

  }

  @Test
  public void testSendReceive() throws SQLException, IOException {

    makeValue();

    String typeCast = typeCasts.get(typeName);
    if (typeCast == null) {
      typeCast = typeName;
    }

    try (PreparedStatement stmt = conn.prepareStatement("SELECT ?::" + typeCast)) {

      stmt.setObject(1, value);

      try (ResultSet rs = stmt.executeQuery()) {

        rs.next();

        if (value instanceof Object[])
          testArray(value, rs.getArray(1));
        else
          test(value, rs.getObject(1));

      }

    }

  }

  void makeValue() {

    if (value instanceof Maker) {
      value = ((Maker) value).make(conn);
    }

  }

  Object coerceValue(Type type, Format format, Object value) throws SQLException {
    Object res;
    if (value instanceof Object[]) {
      Object[] srcArray = (Object[]) value;
      Object[] dstArray = new Object[srcArray.length];
      for (int c = 0; c < srcArray.length; ++c) {
        Class<?> targetType = ((ArrayType)type).getElementType().getCodec(format).encoder.getInputType();
        dstArray[c] = SQLTypeUtils.coerce(format, srcArray[c], ((ArrayType) type).getElementType(), targetType, conn.getTypeMap(), TimeZone.getDefault(), conn);
      }
      res = dstArray;
    }
    else {
      Class<?> targetType = type.getCodec(format).encoder.getInputType();
      res = SQLTypeUtils.coerce(format, value, type, targetType, conn.getTypeMap(), TimeZone.getDefault(), conn);
    }
    return res;
  }

  private Object inOut(Type type, Format format, Object value) throws IOException, SQLException {

    value = coerceValue(type, format, value);

    Codec codec = type.getCodec(format);

    ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
    codec.encoder.encode(type, buffer, value, conn);
    Object res = codec.decoder.decode(type, null, null, buffer, conn);

    if (res instanceof Object[]) {
      Object[] resSrcArray = (Object[]) res;
      Object[] resDstArray = new Object[resSrcArray.length];
      for (int c = 0; c < resSrcArray.length; ++c) {
        Class<?> targetType = SQLTypeUtils.mapGetType(((ArrayType) type).getElementType(), Collections.<String, Class<?>> emptyMap(), conn);
        resDstArray[c] = SQLTypeUtils.coerce(format, resSrcArray[c], ((ArrayType) type).getElementType(), targetType, conn.getTypeMap(), TimeZone.getDefault(), conn);
      }
      res = resDstArray;
    }
    else {
      Class<?> targetType = SQLTypeUtils.mapGetType(type, Collections.<String, Class<?>> emptyMap(), conn);
      res = SQLTypeUtils.coerce(res, type, targetType, Collections.<String, Class<?>> emptyMap(), conn);
    }

    return res;
  }

  public void testArray(Object expected, Array actual) throws SQLException, IOException {
    test(expected, actual.getArray());
  }

  public void test(Object expected, Object actual) throws IOException, SQLException {

    if (expected instanceof InputStream) {
      assertStreamEquals((InputStream) expected, (InputStream) actual);
    }
    else if (expected instanceof byte[]) {
      assertArrayEquals((byte[]) expected, (byte[]) actual);
    }
    else if (expected instanceof Object[]) {
      Object[] expectedArray = (Object[]) expected;
      Object[] actualArray = (Object[]) actual;
      assertEquals("Array Length", expectedArray.length, actualArray.length);
      for (int c = 0; c < expectedArray.length; ++c) {
        test(expectedArray[c], actualArray[c]);
      }
    }
    else if (expected instanceof Struct) {

      Struct expectedStruct = (Struct) expected;
      Object[] expectedAttrs = expectedStruct.getAttributes();

      Struct actualStruct = (Struct) actual;
      Object[] actualAttrs = actualStruct.getAttributes();

      assertEquals("Record Length", expectedAttrs.length, actualAttrs.length);
      for (int c = 0; c < expectedAttrs.length; ++c) {
        test(expectedAttrs[c], actualAttrs[c]);
      }
    }
    else if (expected instanceof Record) {

      Record expectedStruct = (Record) expected;
      Object[] expectedAttrs = expectedStruct.getValues();

      Record actualStruct = (Record) actual;
      Object[] actualAttrs = actualStruct.getValues();

      assertEquals("Record Length", expectedAttrs.length, actualAttrs.length);
      for (int c = 0; c < expectedAttrs.length; ++c) {
        test(expectedAttrs[c], actualAttrs[c]);
      }
    }
    else {
      assertEquals(expected, actual);
    }

  }

  private void compareLengths(Object val, Type type, Codec codec) throws IOException {

    // Compute length with encoder
    int length = codec.encoder.length(type, val, conn);

    // Compute length using null channel buffer
    NullChannelBuffer lengthComputer = new NullChannelBuffer();
    codec.encoder.encode(type, lengthComputer, val, conn);

    assertEquals(typeName + " computes length incorrectly", lengthComputer.readableBytes(), length);
  }

  private void assertStreamEquals(InputStream expected, InputStream actual) throws IOException {
    expected.reset();
    actual.reset();
    assertArrayEquals(ByteStreams.toByteArray(expected), ByteStreams.toByteArray(actual));
  }

  interface Maker {
    Object make(PGConnectionImpl conn);
  }

  @Parameters(name = "test-{0}")
  @SuppressWarnings("deprecation")
  public static Collection<Object[]> data() throws Exception {
    Object[][] scalarTypesData = new Object[][] {
      {"aclitem", new ACLItem("pgjdbc", "rw", "postgres")},
      {"bit", BitSet.valueOf(new byte[] {(byte) 0x7f})},
      {"varbit", BitSet.valueOf(new byte[] {(byte) 0xff, (byte) 0xff})},
      {"bool", true},
      {"bytea", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          return  new ByteArrayInputStream(new byte[5]);
        }

      } },
      {"date", new Date(2000, 1, 1)},
      {"float4", 1.23f},
      {"int2", (short)234},
      {"float8", 2.34d},
      {"int8", (long)234},
      {"int4", 234},
      {"interval", new Interval(1, 2, 3)},
      {"money", new BigDecimal("2342.00")},
      {"name", "hi"},
      {"numeric", new BigDecimal("2342.00")},
      {"oid", 132},
      {"int4range", Range.create(0, true, 5, false)},
      {"text", "hi',\""},
      {"timestamp", new Timestamp(2000, 1, 1, 0, 0, 0, 123000)},
      {"timestamptz", new Timestamp(2000, 1, 1, 0, 0, 0, 123000)},
      {"time", new Time(9, 30, 30)},
      {"timetz", new Time(9, 30, 30)},
      {"teststruct", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          try {
            return conn.createStruct("teststruct", new Object[] {"hi", "hello", UUID.randomUUID(), 2.d});
          }
          catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }

      } },
      {"uuid", UUID.randomUUID()},
      {"xml", new Maker() {

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
      {"macaddr", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          byte[] addr = new byte[6];
          new Random().nextBytes(addr);
          return addr;
        }

      } },
      {"hstore", new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {
          Map<String, String> map = new HashMap<>();
          map.put("1", "one");
          map.put("2", "two");
          map.put("3", "three");
          return map;
        }

      } },
      {"inet", new InetAddr("2001:4f8:3:ba:2e0:81ff:fe22:d1f1/10")},
      {"cidr", new CidrAddr("2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128")},
    };

    List<Object[]> data = new ArrayList<>();

    //Combine entries with generated ones for array and composite testing
    for (final Object[] scalarTypeData : scalarTypesData) {

      final String typeName = (String) scalarTypeData[0];

      final Object typeValue = scalarTypeData[1];

      // Scalar entry

      data.add(scalarTypeData);

      // Array entry

      final String arrayTypeName = typeName + "[]";

      data.add(new Object[] {arrayTypeName, new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {

          Object value = typeValue;
          if (value instanceof Maker)
            value = ((Maker) value).make(conn);

          return new Object[] {value};
        }

      } });

      // Composite entry

      final String structTypeName = typeName + "struct";

      data.add(new Object[] {structTypeName, new Maker() {

        @Override
        public Object make(PGConnectionImpl conn) {

          try {
            String elemTypeName = typeName;
            if (typeCasts.containsKey(typeName)) {
              elemTypeName = typeCasts.get(typeName);
            }

            TestUtil.createType(conn, structTypeName, "elem " + elemTypeName);
          }
          catch (SQLException e) {
            throw new RuntimeException(e);
          }

          conn.getRegistry().unloadType(structTypeName);

          Object value = typeValue;
          if (value instanceof Maker)
            value = ((Maker) value).make(conn);

          try {
            return conn.createStruct(structTypeName, new Object[] {value});
          }
          catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }

      } });
    }

    return data;
  }

  static Map<String, String> typeCasts;
  static {
    typeCasts = new HashMap<>();
    typeCasts.put("bit", "bit(7)");
    typeCasts.put("bit[]", "bit(7)[]");
  }

}
