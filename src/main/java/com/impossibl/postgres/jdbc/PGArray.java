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
import com.impossibl.postgres.system.TypeMapContext;
import com.impossibl.postgres.types.ArrayType;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public abstract class PGArray implements Array {

  protected Context context;
  protected ArrayType type;

  PGArray(Context context, ArrayType type) {
    super();
    this.context = context;
    this.type = type;
  }

  public abstract int getLength();

  protected abstract Object getArray(Context context, Class<?> targetComponentType, long index, int count) throws SQLException;

  protected abstract ResultSet getResultSet(Context context, long index, int count) throws SQLException;

  protected void checkFreed() throws SQLException {
    if (context == null && type == null) {
      throw new PGSQLSimpleException("Array previously freed");
    }
  }

  public ArrayType getType() {
    return type;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    checkFreed();

    return type.getElementType().getName();
  }

  @Override
  public int getBaseType() throws SQLException {
    checkFreed();

    return JDBCTypeMapping.getJDBCTypeCode(type.getElementType());
  }

  public Object getArray(Class<?> targetComponentType) throws SQLException {
    checkFreed();

    return getArray(context, targetComponentType, 1, getLength());
  }

  @Override
  public Object getArray() throws SQLException {
    checkFreed();

    return getArray(context, null, 1, getLength());
  }

  @Override
  public Object getArray(Map<String, Class<?>> typeMap) throws SQLException {
    checkFreed();

    return getArray(new TypeMapContext(context, typeMap), null, 1, getLength());
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    checkFreed();

    return getArray(context, null, index, count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> typeMap) throws SQLException {
    checkFreed();

    return getArray(new TypeMapContext(context, typeMap), null, index, count);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    checkFreed();

    return getResultSet(context.getCustomTypeMap());
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> typeMap) throws SQLException {
    checkFreed();

    return getResultSet(new TypeMapContext(context, typeMap), 1, getLength());
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    checkFreed();

    return getResultSet(context, index, count);
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> typeMap) throws SQLException {
    checkFreed();

    return getResultSet(new TypeMapContext(context, typeMap), index, count);
  }

}
