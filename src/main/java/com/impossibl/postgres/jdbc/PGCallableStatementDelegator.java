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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * CallableStatement delegator
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGCallableStatementDelegator extends PGPreparedStatementDelegator implements CallableStatement {
  private PGPooledConnectionDelegator owner;
  private CallableStatement delegator;

  /**
   * Constructor
   * @param owner The owner
   * @param delegator The delegator
   */
  public PGCallableStatementDelegator(PGPooledConnectionDelegator owner, CallableStatement delegator) {
    super(owner, delegator);
    this.owner = owner;
    this.delegator = delegator;
  }

  /**
   * {@inheritDoc}
   */
  public Array getArray(int parameterIndex) throws SQLException {
    try {
      return delegator.getArray(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Array getArray(String parameterName) throws SQLException {
    try {
      return delegator.getArray(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    try {
      return delegator.getBigDecimal(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    try {
      return delegator.getBigDecimal(parameterIndex, scale);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    try {
      return delegator.getBigDecimal(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Blob getBlob(int parameterIndex) throws SQLException {
    try {
      return delegator.getBlob(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Blob getBlob(String parameterName) throws SQLException {
    try {
      return delegator.getBlob(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean getBoolean(int parameterIndex) throws SQLException {
    try {
      return delegator.getBoolean(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean getBoolean(String parameterName) throws SQLException {
    try {
      return delegator.getBoolean(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public byte getByte(int parameterIndex) throws SQLException {
    try {
      return delegator.getByte(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public byte getByte(String parameterName) throws SQLException {
    try {
      return delegator.getByte(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public byte[] getBytes(int parameterIndex) throws SQLException {
    try {
      return delegator.getBytes(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public byte[] getBytes(String parameterName) throws SQLException {
    try {
      return delegator.getBytes(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Reader getCharacterStream(int parameterIndex) throws SQLException {
    try {
      return delegator.getCharacterStream(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Reader getCharacterStream(String parameterName) throws SQLException {
    try {
      return delegator.getCharacterStream(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Clob getClob(int parameterIndex) throws SQLException {
    try {
      return delegator.getClob(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Clob getClob(String parameterName) throws SQLException {
    try {
      return delegator.getClob(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Date getDate(int parameterIndex) throws SQLException {
    try {
      return delegator.getDate(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    try {
      return delegator.getDate(parameterIndex, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Date getDate(String parameterName) throws SQLException {
    try {
      return delegator.getDate(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Date getDate(String parameterName, Calendar cal) throws SQLException {
    try {
      return delegator.getDate(parameterName, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public double getDouble(int parameterIndex) throws SQLException {
    try {
      return delegator.getDouble(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public double getDouble(String parameterName) throws SQLException {
    try {
      return delegator.getDouble(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public float getFloat(int parameterIndex) throws SQLException {
    try {
      return delegator.getFloat(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public float getFloat(String parameterName) throws SQLException {
    try {
      return delegator.getFloat(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public int getInt(int parameterIndex) throws SQLException {
    try {
      return delegator.getInt(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public int getInt(String parameterName) throws SQLException {
    try {
      return delegator.getInt(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public long getLong(int parameterIndex) throws SQLException {
    try {
      return delegator.getLong(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public long getLong(String parameterName) throws SQLException {
    try {
      return delegator.getLong(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    try {
      return delegator.getNCharacterStream(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Reader getNCharacterStream(String parameterName) throws SQLException {
    try {
      return delegator.getNCharacterStream(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public NClob getNClob(int parameterIndex) throws SQLException {
    try {
      return delegator.getNClob(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public NClob getNClob(String parameterName) throws SQLException {
    try {
      return delegator.getNClob(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getNString(int parameterIndex) throws SQLException {
    try {
      return delegator.getNString(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getNString(String parameterName) throws SQLException {
    try {
      return delegator.getNString(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Object getObject(int parameterIndex) throws SQLException {
    try {
      return delegator.getObject(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    try {
      return delegator.getObject(parameterIndex, type);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
    try {
      return delegator.getObject(parameterIndex, map);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Object getObject(String parameterName) throws SQLException {
    try {
      return delegator.getObject(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    try {
      return delegator.getObject(parameterName, type);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
    try {
      return delegator.getObject(parameterName, map);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Ref getRef(int parameterIndex) throws SQLException {
    try {
      return delegator.getRef(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Ref getRef(String parameterName) throws SQLException {
    try {
      return delegator.getRef(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public RowId getRowId(int parameterIndex) throws SQLException {
    try {
      return delegator.getRowId(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public RowId getRowId(String parameterName) throws SQLException {
    try {
      return delegator.getRowId(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public short getShort(int parameterIndex) throws SQLException {
    try {
      return delegator.getShort(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public short getShort(String parameterName) throws SQLException {
    try {
      return delegator.getShort(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    try {
      return delegator.getSQLXML(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public SQLXML getSQLXML(String parameterName) throws SQLException {
    try {
      return delegator.getSQLXML(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getString(int parameterIndex) throws SQLException {
    try {
      return delegator.getString(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public String getString(String parameterName) throws SQLException {
    try {
      return delegator.getString(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Time getTime(int parameterIndex) throws SQLException {
    try {
      return delegator.getTime(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    try {
      return delegator.getTime(parameterIndex, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Time getTime(String parameterName) throws SQLException {
    try {
      return delegator.getTime(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    try {
      return delegator.getTime(parameterName, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Timestamp getTimestamp(int parameterIndex) throws SQLException {
    try {
      return delegator.getTimestamp(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
    try {
      return delegator.getTimestamp(parameterIndex, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Timestamp getTimestamp(String parameterName) throws SQLException {
    try {
      return delegator.getTimestamp(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    try {
      return delegator.getTimestamp(parameterName, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public URL getURL(int parameterIndex) throws SQLException {
    try {
      return delegator.getURL(parameterIndex);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public URL getURL(String parameterName) throws SQLException {
    try {
      return delegator.getURL(parameterName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
    try {
      delegator.registerOutParameter(parameterIndex, sqlType);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
    try {
      delegator.registerOutParameter(parameterIndex, sqlType, scale);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
    try {
      delegator.registerOutParameter(parameterIndex, sqlType, typeName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    try {
      delegator.registerOutParameter(parameterName, sqlType);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
    try {
      delegator.registerOutParameter(parameterName, sqlType, scale);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
    try {
      delegator.registerOutParameter(parameterName, sqlType, typeName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
    try {
      delegator.setAsciiStream(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    try {
      delegator.setAsciiStream(parameterName, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
    try {
      delegator.setAsciiStream(parameterName, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    try {
      delegator.setBigDecimal(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
    try {
      delegator.setBinaryStream(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
    try {
      delegator.setBinaryStream(parameterName, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
    try {
      delegator.setBinaryStream(parameterName, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBlob(String parameterName, Blob x) throws SQLException {
    try {
      delegator.setBlob(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    try {
      delegator.setBlob(parameterName, inputStream);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
    try {
      delegator.setBlob(parameterName, inputStream, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBoolean(String parameterName, boolean x) throws SQLException {
    try {
      delegator.setBoolean(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setByte(String parameterName, byte x) throws SQLException {
    try {
      delegator.setByte(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBytes(String parameterName, byte[] x) throws SQLException {
    try {
      delegator.setBytes(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
    try {
      delegator.setCharacterStream(parameterName, reader);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
    try {
      delegator.setCharacterStream(parameterName, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
    try {
      delegator.setCharacterStream(parameterName, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setClob(String parameterName, Clob x) throws SQLException {
    try {
      delegator.setClob(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setClob(String parameterName, Reader reader) throws SQLException {
    try {
      delegator.setClob(parameterName, reader);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setClob(String parameterName, Reader reader, long length) throws SQLException {
    try {
      delegator.setClob(parameterName, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setDate(String parameterName, Date x) throws SQLException {
    try {
      delegator.setDate(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
    try {
      delegator.setDate(parameterName, x, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setDouble(String parameterName, double x) throws SQLException {
    try {
      delegator.setDouble(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setFloat(String parameterName, float x) throws SQLException {
    try {
      delegator.setFloat(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setInt(String parameterName, int x) throws SQLException {
    try {
      delegator.setInt(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setLong(String parameterName, long x) throws SQLException {
    try {
      delegator.setLong(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    try {
      delegator.setNCharacterStream(parameterName, value);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
    try {
      delegator.setNCharacterStream(parameterName, value, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNClob(String parameterName, NClob value) throws SQLException {
    try {
      delegator.setNClob(parameterName, value);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNClob(String parameterName, Reader reader) throws SQLException {
    try {
      delegator.setNClob(parameterName, reader);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    try {
      delegator.setNClob(parameterName, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNString(String parameterName, String value) throws SQLException {
    try {
      delegator.setNString(parameterName, value);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNull(String parameterName, int sqlType) throws SQLException {
    try {
      delegator.setNull(parameterName, sqlType);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    try {
      delegator.setNull(parameterName, sqlType, typeName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setObject(String parameterName, Object x) throws SQLException {
    try {
      delegator.setObject(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    try {
      delegator.setObject(parameterName, x, targetSqlType);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
    try {
      delegator.setObject(parameterName, x, targetSqlType, scale);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setRowId(String parameterName, RowId x) throws SQLException {
    try {
      delegator.setRowId(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setShort(String parameterName, short x) throws SQLException {
    try {
      delegator.setShort(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    try {
      delegator.setSQLXML(parameterName, xmlObject);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setString(String parameterName, String x) throws SQLException {
    try {
      delegator.setString(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTime(String parameterName, Time x) throws SQLException {
    try {
      delegator.setTime(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    try {
      delegator.setTime(parameterName, x, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    try {
      delegator.setTimestamp(parameterName, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    try {
      delegator.setTimestamp(parameterName, x, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setURL(String parameterName, URL val) throws SQLException {
    try {
      delegator.setURL(parameterName, val);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean wasNull() throws SQLException {
    try {
      return delegator.wasNull();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }
}
