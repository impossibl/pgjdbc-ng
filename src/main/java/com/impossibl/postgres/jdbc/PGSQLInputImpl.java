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

import com.impossibl.postgres.api.jdbc.PGSQLInput;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.CompositeType.Attribute;
import com.impossibl.postgres.utils.guava.ByteStreams;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerce;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBigDecimal;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBlob;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToBoolean;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToByte;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToByteStream;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToClob;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToDate;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToDouble;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToFloat;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToInt;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToLong;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToRowId;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToShort;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToString;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToTime;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToTimestamp;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToURL;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.coerceToXML;
import static com.impossibl.postgres.jdbc.SQLTypeUtils.mapGetType;

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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TimeZone;

import static java.nio.charset.StandardCharsets.US_ASCII;



public class PGSQLInputImpl implements PGSQLInput {

  private PGConnectionImpl connection;
  private CompositeType type;
  private Map<String, Class<?>> typeMap;
  private int currentAttrIdx;
  private Object[] attributeValues;
  private Boolean nullFlag;

  public PGSQLInputImpl(PGConnectionImpl connection, CompositeType type, Map<String, Class<?>> typeMap, Object[] attributeValues) {
    this.connection = connection;
    this.type = type;
    this.typeMap = typeMap;
    this.attributeValues = attributeValues;
  }

  public Object[] getAttributeValues() {
    return attributeValues;
  }

  private Object getNextAttributeValue() {

    Object val = attributeValues[currentAttrIdx++];
    nullFlag = val == null;
    return val;
  }

  @Override
  public String readString() throws SQLException {
    return coerceToString(getNextAttributeValue(), type, connection);
  }

  @Override
  public boolean readBoolean() throws SQLException {
    return coerceToBoolean(getNextAttributeValue());
  }

  @Override
  public byte readByte() throws SQLException {
    return coerceToByte(getNextAttributeValue());
  }

  @Override
  public short readShort() throws SQLException {
    return coerceToShort(getNextAttributeValue());
  }

  @Override
  public int readInt() throws SQLException {
    return coerceToInt(getNextAttributeValue());
  }

  @Override
  public long readLong() throws SQLException {
    return coerceToLong(getNextAttributeValue());
  }

  @Override
  public float readFloat() throws SQLException {
    return coerceToFloat(getNextAttributeValue());
  }

  @Override
  public double readDouble() throws SQLException {
    return coerceToDouble(getNextAttributeValue());
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return coerceToBigDecimal(getNextAttributeValue());
  }

  @Override
  public byte[] readBytes() throws SQLException {

    Object val = getNextAttributeValue();
    if (val == null) {
      return null;
    }

    Attribute attr = type.getAttribute(currentAttrIdx);
    if (attr == null) {
      throw new SQLException("Invalid input request (type not array)");
    }

    InputStream data = coerceToByteStream(getNextAttributeValue(), attr.type, connection);

    try {
      return ByteStreams.toByteArray(data);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Date readDate() throws SQLException {
    return coerceToDate(getNextAttributeValue(), TimeZone.getDefault(), connection);
  }

  @Override
  public Time readTime() throws SQLException {

    return coerceToTime(getNextAttributeValue(), TimeZone.getDefault(), connection);
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {

    return coerceToTimestamp(getNextAttributeValue(), TimeZone.getDefault(), connection);
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    return new StringReader(coerceToString(getNextAttributeValue(), type, connection));
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    return new ByteArrayInputStream(coerceToString(getNextAttributeValue(), type, connection).getBytes(US_ASCII));
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    return new ByteArrayInputStream(readBytes());
  }

  @Override
  public Object readObject() throws SQLException {

    Object val = getNextAttributeValue();
    if (val == null) {
      return null;
    }

    Attribute attr = type.getAttribute(currentAttrIdx);
    if (attr == null) {
      throw new SQLException("Invalid input request (type not array)");
    }

    Class<?> targetType = mapGetType(attr.type, typeMap, connection);

    return coerce(val, attr.type, targetType, typeMap, connection);
  }

  @Override
  public Ref readRef() throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public Blob readBlob() throws SQLException {
    return coerceToBlob(getNextAttributeValue(), connection);
  }

  @Override
  public Clob readClob() throws SQLException {
    return coerceToClob(getNextAttributeValue(), connection);
  }

  @Override
  public Array readArray() throws SQLException {

    Object val = getNextAttributeValue();
    if (val == null) {
      return null;
    }

    Attribute attr = type.getAttribute(currentAttrIdx);
    if (attr == null || !(attr.type instanceof ArrayType) || !(val instanceof Object[])) {
      throw new SQLException("Invalid input request (type not array)");
    }

    return new PGArray(connection, (ArrayType)attr.type, (Object[])val);
  }

  @Override
  public URL readURL() throws SQLException {
    return coerceToURL(getNextAttributeValue());
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    return coerceToXML(getNextAttributeValue(), connection);
  }

  @Override
  public RowId readRowId() throws SQLException {
    return coerceToRowId(getNextAttributeValue(), type.getAttribute(currentAttrIdx).type);
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
