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

import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.Exceptions.PARAMETER_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.system.CustomTypes.lookupCustomType;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Map;

public class PGParameterMetaData implements ParameterMetaData {


  private Type[] parameterTypes;
  private Map<String, Class<?>> typeMap;


  PGParameterMetaData(Type[] parameterTypes, Map<String, Class<?>> typeMap) {
    super();
    this.parameterTypes = parameterTypes;
    this.typeMap = typeMap;
  }

  private void checkParamIndex(int paramIndex) throws SQLException {

    if (paramIndex < 1 || paramIndex > parameterTypes.length)
      throw PARAMETER_INDEX_OUT_OF_BOUNDS;

  }

  private Type getType(int paramIndex) throws SQLException {
    checkParamIndex(paramIndex);

    return parameterTypes[paramIndex - 1];
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (!iface.isAssignableFrom(getClass())) {
      throw UNWRAP_ERROR;
    }

    return iface.cast(this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(getClass());
  }

  @Override
  public int getParameterCount() {
    return parameterTypes.length;
  }

  @Override
  public int isNullable(int param) throws SQLException {

    Type paramType = getType(param);

    if (SQLTypeMetaData.isNullable(paramType) == parameterNoNulls) {
      return parameterNoNulls;
    }

    return ParameterMetaData.parameterNullable;
  }

  @Override
  public boolean isSigned(int param) throws SQLException {

    Type type = getType(param);

    return SQLTypeMetaData.isSigned(type);
  }

  @Override
  public int getPrecision(int param) throws SQLException {

    Type paramType = getType(param);

    return SQLTypeMetaData.getPrecision(paramType, 0, 0);
  }

  @Override
  public int getScale(int param) throws SQLException {

    Type paramType = getType(param);

    return SQLTypeMetaData.getScale(paramType, 0);
  }

  @Override
  public int getParameterType(int param) throws SQLException {

    Type paramType = getType(param);

    return SQLTypeMetaData.getSQLType(paramType);
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {

    Type paramType = getType(param);

    return paramType.getName();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {

    Type paramType = getType(param);

    Class<?> defaultClass = paramType.getCodec(paramType.getParameterFormat()).getDecoder().getDefaultClass();
    Class<?> paramClass = lookupCustomType(paramType, typeMap, defaultClass);
    return paramClass.getName();
  }

  @Override
  public int getParameterMode(int param) {

    return ParameterMetaData.parameterModeIn;
  }

}
