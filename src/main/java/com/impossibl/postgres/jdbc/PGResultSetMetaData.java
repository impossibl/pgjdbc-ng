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
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.Exceptions.COLUMN_INDEX_OUT_OF_BOUNDS;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.system.CustomTypes.lookupCustomType;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_NAME;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

class PGResultSetMetaData extends PGMetaData implements ResultSetMetaData {

  private ResultField[] resultFields;
  private Map<Integer, List<ColumnData>> relationsColumnsData;
  private Map<String, Class<?>> typeMap;

  PGResultSetMetaData(PGDirectConnection connection, ResultField[] resultFields, Map<String, Class<?>> typeMap) {
    super(connection);
    this.resultFields = resultFields;
    this.relationsColumnsData = new HashMap<>();
    this.typeMap = typeMap;
  }

  private ColumnData getRelationColumnData(ResultField field) throws SQLException {
    List<ColumnData> relationColumnsData = getRelationColumnsData(field.getRelationId());
    for (ColumnData relationColumnData : relationColumnsData) {
      if (relationColumnData.relationAttrNum == field.getRelationAttributeNumber()) {
        return relationColumnData;
      }
    }
    return null;
  }

  private List<ColumnData> getRelationColumnsData(int relationId) throws SQLException {
    List<ColumnData> data;
    if ((data = relationsColumnsData.get(relationId)) == null) {
      data = loadRelationColumsData(relationId);
      relationsColumnsData.put(relationId, data);
    }
    return data;
  }

  private List<ColumnData> loadRelationColumsData(int relationId) throws SQLException {
    String sql = getColumnSQL(" AND a.attnum > 0 AND c.oid = ?").toString();

    return getColumnData(sql, singletonList(relationId));
  }

  /**
   * Returns the ResultField associated with the requested column
   *
   * @param columnIndex Requested column index
   * @return ResultField of column
   * @throws SQLException If columnIndex is out of bounds
   */
  ResultField get(int columnIndex) throws SQLException {

    if (columnIndex < 1 || columnIndex > resultFields.length) {
      throw COLUMN_INDEX_OUT_OF_BOUNDS;
    }

    return resultFields[columnIndex - 1];
  }

  /**
   * Returns the CompositeType representing the requested column's table.
   *
   * @param columnIndex Requested column index
   * @return CompositeType of columns table
   * @throws SQLException If columnIndex is out of bounds
   */
  private CompositeType getRelType(int columnIndex) throws SQLException {

    ResultField field = get(columnIndex);
    if (field.getRelationId() == 0)
      return null;

    try {
      return connection.getRegistry().loadRelationType(field.getRelationId());
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
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
  public int getColumnCount() {
    return resultFields.length;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {

    ResultField field = get(column);
    ColumnData columnData = getRelationColumnData(field);
    if (columnData == null) {
      try {
        return connection.getRegistry().resolve(field.getTypeRef()).isAutoIncrement();
      }
      catch (IOException e) {
        throw makeSQLException(e);
      }
    }

    return Type.isAutoIncrement(columnData.defaultValue) || columnData.type.isAutoIncrement();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {

    Registry registry = connection.getRegistry();

    try {
      return JDBCTypeMetaData.isCaseSensitive(registry.resolve(get(column).getTypeRef()));
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  @Override
  public boolean isSearchable(int column) {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {

    try {
      return JDBCTypeMetaData.isCurrency(connection.getRegistry().resolve(get(column).getTypeRef()));
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  @Override
  public int isNullable(int column) throws SQLException {

    ResultField field = get(column);
    ColumnData columnData = getRelationColumnData(field);

    Boolean nullable;
    if (columnData == null) {
      try {
        nullable = connection.getRegistry().resolve(field.getTypeRef()).isNullable();
      }
      catch (IOException e) {
        throw makeSQLException(e);
      }
    }
    else {
      nullable = columnData.nullable != null ? columnData.nullable : columnData.type.isNullable();
    }

    return nullable != null ? (nullable ? columnNullable : columnNoNulls) : columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {

    try {
      return JDBCTypeMetaData.isSigned(connection.getRegistry().resolve(get(column).getTypeRef()));
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {

    String val = get(column).getName();
    if (val == null)
      val = getColumnName(column);

    return val;
  }

  @Override
  public String getCatalogName(int column) {

    return connection.getSetting(DATABASE_NAME);
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

    ResultField field = get(column);
    ColumnData columnData = getRelationColumnData(field);
    if (columnData == null) {
      return get(column).getName();
    }

    return columnData.columnName;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    ResultField field = get(column);
    try {
      return JDBCTypeMapping.getSQLTypeCode(connection.getRegistry().resolve(field.getTypeRef()));
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {

    ResultField field = get(column);
    ColumnData columnData = getRelationColumnData(field);
    if (columnData == null) {
      try {
        return connection.getRegistry().resolve(field.getTypeRef()).getName();
      }
      catch (IOException e) {
        throw makeSQLException(e);
      }
    }

    return JDBCTypeMetaData.getTypeName(columnData.type, columnData.defaultValue);
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {

    Type type;
    try {
      type = connection.getRegistry().resolve(get(column).getTypeRef());
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
    return lookupCustomType(type, typeMap, type.getCodec(type.getResultFormat()).getDecoder().getDefaultClass()).getName();
  }

  @Override
  public int getPrecision(int column) throws SQLException {

    ResultField field = get(column);
    try {
      return JDBCTypeMetaData.getPrecision(connection.getRegistry().resolve(field.getTypeRef()), field.getTypeLength(), field.getTypeModifier());
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  @Override
  public int getScale(int column) throws SQLException {

    ResultField field = get(column);
    try {
      return JDBCTypeMetaData.getScale(connection.getRegistry().resolve(field.getTypeRef()), field.getTypeModifier());
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {

    ResultField field = get(column);
    try {
      return JDBCTypeMetaData.getDisplaySize(connection.getRegistry().resolve(field.getTypeRef()), field.getTypeLength(), field.getTypeModifier());
    }
    catch (IOException e) {
      throw makeSQLException(e);
    }
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
