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

import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.system.Settings;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.Exceptions.COLUMN_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.jdbc.SQLTypeMetaData.getSQLType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

class PGResultSetMetaData implements ResultSetMetaData {

  PGConnectionImpl connection;
  List<ResultField> resultFields;
  Map<String, Class<?>> typeMap;

  PGResultSetMetaData(PGConnectionImpl connection, List<ResultField> resultFields, Map<String, Class<?>> typeMap) {
    this.connection = connection;
    this.resultFields = resultFields;
    this.typeMap = typeMap;
  }

  /**
   * Returns the ResultField associated with the requested column
   *
   * @param columnIndex Requested column index
   * @return ResultField of column
   * @throws SQLException If columnIndex is out of bounds
   */
  ResultField get(int columnIndex) throws SQLException {

    if (columnIndex < 1 || columnIndex > resultFields.size()) {
      throw COLUMN_INDEX_OUT_OF_BOUNDS;
    }

    return resultFields.get(columnIndex - 1);
  }

  /**
   * Returns the CompositeType representing the requested column's table.
   *
   * @param columnIndex Requested column index
   * @return CompositeType of columns table
   * @throws SQLException If columnIndex is out of bounds
   */
  CompositeType getRelType(int columnIndex) throws SQLException {

    ResultField field = get(columnIndex);
    if (field.getRelationId() == 0)
      return null;

    return connection.getRegistry().loadRelationType(field.getRelationId());
  }

  /**
   * Returns the CompositeType.Attribute representing the requested column
   *
   * @param columnIndex Requested column index
   * @return CompositeType.Attribute of the requested column
   * @throws SQLException If columnIndex is out of bounds
   */
  CompositeType.Attribute getRelAttr(int columnIndex) throws SQLException {

    ResultField field = get(columnIndex);

    CompositeType relType = connection.getRegistry().loadRelationType(field.getRelationId());
    if (relType == null)
      return null;

    return relType.getAttribute(field.getRelationAttributeNumber());
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (!iface.isAssignableFrom(getClass())) {
      throw UNWRAP_ERROR;
    }

    return iface.cast(this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  @Override
  public int getColumnCount() throws SQLException {
    return resultFields.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {

    ResultField field = get(column);
    CompositeType relType = connection.getRegistry().loadRelationType(field.getRelationId());

    return SQLTypeMetaData.isAutoIncrement(field.getTypeRef().get(), relType, field.getRelationAttributeNumber());
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {

    return SQLTypeMetaData.isCaseSensitive(get(column).getTypeRef().get());
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {

    return SQLTypeMetaData.isCurrency(get(column).getTypeRef().get());
  }

  @Override
  public int isNullable(int column) throws SQLException {

    ResultField field = get(column);
    CompositeType relType = connection.getRegistry().loadRelationType(field.getRelationId());

    return SQLTypeMetaData.isNullable(field.getTypeRef().get(), relType, field.getRelationAttributeNumber());
  }

  @Override
  public boolean isSigned(int column) throws SQLException {

    return SQLTypeMetaData.isSigned(get(column).getTypeRef().get());
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {

    String val = get(column).getName();
    if (val == null)
      val = getColumnName(column);

    return val;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {

    return connection.getSetting(Settings.DATABASE).toString();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {

    Type relType = getRelType(column);
    if (relType == null)
      return "";

    return relType.getNamespace();
  }

  @Override
  public String getTableName(int column) throws SQLException {

    //Note: there seems to be some debate about whether this should return
    //query aliases or table names. We are returning table names, if
    //available, as this is at least more useful than always returning
    //null since we never have the query table aliases

    CompositeType relType = getRelType(column);
    if (relType == null)
      return "";

    return relType.getName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    if (connection.isStrictMode()) {
      String val = get(column).getName();
      if (val != null)
        return val;
    }

    CompositeType.Attribute attr = getRelAttr(column);
    if (attr == null)
      return get(column).getName();

    return attr.getName();
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    ResultField field = get(column);
    return getSQLType(field.getTypeRef().get());
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {

    ResultField field = get(column);
    CompositeType relType = connection.getRegistry().loadRelationType(field.getRelationId());

    return SQLTypeMetaData.getTypeName(field.getTypeRef().get(), relType, field.getRelationAttributeNumber());
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {

    ResultField field = get(column);
    return SQLTypeUtils.mapGetType(field.getTypeRef().get(), typeMap, connection).getName();
  }

  @Override
  public int getPrecision(int column) throws SQLException {

    ResultField field = get(column);
    return SQLTypeMetaData.getPrecision(field.getTypeRef().get(), field.getTypeLength(), field.getTypeModifier());
  }

  @Override
  public int getScale(int column) throws SQLException {

    ResultField field = get(column);
    return SQLTypeMetaData.getScale(field.getTypeRef().get(), field.getTypeLength(), field.getTypeModifier());
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {

    ResultField field = get(column);
    return SQLTypeMetaData.getDisplaySize(field.getTypeRef().get(), field.getTypeLength(), field.getTypeModifier());
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {

    //If it's a computed column we assume it's read only
    return get(column).getRelationAttributeNumber() == 0;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return !isReadOnly(column);
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    //TODO determine what this is really asking
    return isWritable(column);
  }

}
