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
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * PreparedStatement delegator
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
public class PGPreparedStatementDelegator extends PGStatementDelegator implements PreparedStatement {
  private PGPooledConnectionDelegator owner;
  private PreparedStatement delegator;

  /**
   * Constructor
   * @param owner The owner
   * @param delegator The delegator
   */
  public PGPreparedStatementDelegator(PGPooledConnectionDelegator owner, PreparedStatement delegator) {
    super(owner, delegator);
    this.owner = owner;
    this.delegator = delegator;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws SQLException {
    try {
      delegator.close();
      owner.fireStatementClosed(this);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void addBatch() throws SQLException {
    try {
      delegator.addBatch();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void clearParameters() throws SQLException {
    try {
      delegator.clearParameters();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean execute() throws SQLException {
    try {
      return delegator.execute();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public ResultSet executeQuery() throws SQLException {
    try {
      return delegator.executeQuery();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public int executeUpdate() throws SQLException {
    try {
      return delegator.executeUpdate();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public ResultSetMetaData getMetaData() throws SQLException {
    try {
      return delegator.getMetaData();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public ParameterMetaData getParameterMetaData() throws SQLException {
    try {
      return delegator.getParameterMetaData();
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setArray(int parameterIndex, Array x) throws SQLException {
    try {
      delegator.setArray(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    try {
      delegator.setAsciiStream(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      delegator.setAsciiStream(parameterIndex, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    try {
      delegator.setAsciiStream(parameterIndex, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    try {
      delegator.setBigDecimal(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    try {
      delegator.setBinaryStream(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      delegator.setBinaryStream(parameterIndex, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    try {
      delegator.setBinaryStream(parameterIndex, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    try {
      delegator.setBlob(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    try {
      delegator.setBlob(parameterIndex, inputStream);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    try {
      delegator.setBlob(parameterIndex, inputStream, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    try {
      delegator.setBoolean(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setByte(int parameterIndex, byte x) throws SQLException {
    try {
      delegator.setByte(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    try {
      delegator.setBytes(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    try {
      delegator.setCharacterStream(parameterIndex, reader);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    try {
      delegator.setCharacterStream(parameterIndex, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    try {
      delegator.setCharacterStream(parameterIndex, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    try {
      delegator.setClob(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    try {
      delegator.setClob(parameterIndex, reader);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    try {
      delegator.setClob(parameterIndex, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setDate(int parameterIndex, Date x) throws SQLException {
    try {
      delegator.setDate(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    try {
      delegator.setDate(parameterIndex, x, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setDouble(int parameterIndex, double x) throws SQLException {
    try {
      delegator.setDouble(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setFloat(int parameterIndex, float x) throws SQLException {
    try {
      delegator.setFloat(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setInt(int parameterIndex, int x) throws SQLException {
    try {
      delegator.setInt(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setLong(int parameterIndex, long x) throws SQLException {
    try {
      delegator.setLong(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    try {
      delegator.setNCharacterStream(parameterIndex, value);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    try {
      delegator.setNCharacterStream(parameterIndex, value, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    try {
      delegator.setNClob(parameterIndex, value);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    try {
      delegator.setNClob(parameterIndex, reader);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    try {
      delegator.setNClob(parameterIndex, reader, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNString(int parameterIndex, String value) throws SQLException {
    try {
      delegator.setNString(parameterIndex, value);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    try {
      delegator.setNull(parameterIndex, sqlType);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    try {
      delegator.setNull(parameterIndex, sqlType, typeName);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setObject(int parameterIndex, Object x) throws SQLException {
    try {
      delegator.setObject(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    try {
      delegator.setObject(parameterIndex, x, targetSqlType);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    try {
      delegator.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    try {
      delegator.setRef(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    try {
      delegator.setRowId(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setShort(int parameterIndex, short x) throws SQLException {
    try {
      delegator.setShort(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    try {
      delegator.setSQLXML(parameterIndex, xmlObject);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setString(int parameterIndex, String x) throws SQLException {
    try {
      delegator.setString(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTime(int parameterIndex, Time x) throws SQLException {
    try {
      delegator.setTime(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    try {
      delegator.setTime(parameterIndex, x, cal);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    try {
      delegator.setTimestamp(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    try {
      delegator.setTimestamp(parameterIndex, x, cal);
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
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    try {
      delegator.setUnicodeStream(parameterIndex, x, length);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }

  /**
   * {@inheritDoc}
   */
  public void setURL(int parameterIndex, URL x) throws SQLException {
    try {
      delegator.setURL(parameterIndex, x);
    }
    catch (SQLException se) {
      owner.fireStatementError(this, se);
      throw se;
    }
  }
}
