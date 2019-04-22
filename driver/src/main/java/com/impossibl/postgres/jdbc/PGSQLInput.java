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

import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import static java.nio.charset.StandardCharsets.US_ASCII;

import io.netty.buffer.ByteBuf;


public abstract class PGSQLInput<Buffer> implements SQLInput {

  public static class Binary extends PGSQLInput<ByteBuf> {

    public Binary(Context context, Type[] attributeTypes, ByteBuf[] attributeBuffers) {
      super(context, attributeTypes, attributeBuffers);
    }

    @Override
    protected Type.Codec.Decoder<ByteBuf> getDecoder(Type type) {
      return type.getBinaryCodec().getDecoder();
    }

  }

  public static class Text extends PGSQLInput<CharSequence> {

    public Text(Context context, Type[] attributeTypes, CharSequence[] attributeBuffers) {
      super(context, attributeTypes, attributeBuffers);
    }

    @Override
    protected Type.Codec.Decoder<CharSequence> getDecoder(Type type) {
      return type.getTextCodec().getDecoder();
    }

  }

  private Context context;
  private Type[] attributeTypes;
  private Buffer[] attributeBuffers;
  private int currentAttrIdx;
  private Boolean nullFlag;

  PGSQLInput(Context context, Type[] attributeTypes, Buffer[] attributeBuffers) {
    this.context = context;
    this.attributeTypes = attributeTypes;
    this.attributeBuffers = attributeBuffers;
    this.currentAttrIdx = -1;
  }

  protected abstract Type.Codec.Decoder<Buffer> getDecoder(Type type);

  private <Result> Result getNextAttributeValue(Class<? extends Result> targetClass) throws SQLException {
    return targetClass.cast(getNextAttributeObject(targetClass));
  }

  private Object getNextAttributeObject(Class<?> targetClass) throws SQLException {
    currentAttrIdx++;
    Type type = attributeTypes[currentAttrIdx];
    Buffer buffer = attributeBuffers[currentAttrIdx];
    if (buffer == null) {
      nullFlag = true;
      return null;
    }

    try {
      Object result =
          getDecoder(type).decode(context, type, type.getLength(), null, buffer, targetClass, null);

      nullFlag = result == null;

      return result;
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public String readString() throws SQLException {
    return getNextAttributeValue(String.class);
  }

  @Override
  public boolean readBoolean() throws SQLException {
    Boolean val = getNextAttributeValue(Boolean.class);
    return val != null ? val : false;
  }

  @Override
  public byte readByte() throws SQLException {
    Byte val = getNextAttributeValue(Byte.class);
    return val != null ? val : 0;
  }

  @Override
  public short readShort() throws SQLException {
    Short val = getNextAttributeValue(Short.class);
    return val != null ? val : 0;
  }

  @Override
  public int readInt() throws SQLException {
    Integer val = getNextAttributeValue(Integer.class);
    return val != null ? val : 0;
  }

  @Override
  public long readLong() throws SQLException {
    Long val = getNextAttributeValue(Long.class);
    return val != null ? val : 0;
  }

  @Override
  public float readFloat() throws SQLException {
    Float val = getNextAttributeValue(Float.class);
    return val != null ? val : 0;
  }

  @Override
  public double readDouble() throws SQLException {
    Double val = getNextAttributeValue(Double.class);
    return val != null ? val : 0;
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return getNextAttributeValue(BigDecimal.class);
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return getNextAttributeValue(byte[].class);
  }

  @Override
  public Date readDate() throws SQLException {
    return getNextAttributeValue(Date.class);
  }

  @Override
  public Time readTime() throws SQLException {
    return getNextAttributeValue(Time.class);
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    return getNextAttributeValue(Timestamp.class);
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    return new StringReader(getNextAttributeValue(String.class));
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    return new ByteArrayInputStream(getNextAttributeValue(String.class).getBytes(US_ASCII));
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    return new ByteArrayInputStream(readBytes());
  }

  @Override
  public Object readObject() throws SQLException {
    return getNextAttributeObject(null);
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return getNextAttributeValue(type);
  }

  @Override
  public Ref readRef() throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public Blob readBlob() throws SQLException {
    return getNextAttributeValue(Blob.class);
  }

  @Override
  public Clob readClob() throws SQLException {
    return getNextAttributeValue(Clob.class);
  }

  @Override
  public Array readArray() throws SQLException {
    return getNextAttributeValue(Array.class);
  }

  @Override
  public URL readURL() throws SQLException {
    return getNextAttributeValue(URL.class);
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    return getNextAttributeValue(SQLXML.class);
  }

  @Override
  public RowId readRowId() throws SQLException {
    return getNextAttributeValue(RowId.class);
  }

  @Override
  public boolean wasNull() throws SQLException {

    if (nullFlag == null)
      throw new SQLException("no value read");

    return nullFlag;
  }

  @Override
  public NClob readNClob() throws SQLException {
    throw NOT_SUPPORTED;
  }

  @Override
  public String readNString() throws SQLException {
    throw NOT_SUPPORTED;
  }

}
