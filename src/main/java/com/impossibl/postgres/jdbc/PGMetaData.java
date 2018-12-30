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
import com.impossibl.postgres.protocol.RowDataSet;
import com.impossibl.postgres.protocol.v30.BufferRowData;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;

import static com.impossibl.postgres.jdbc.ErrorUtils.makeSQLException;
import static com.impossibl.postgres.jdbc.Exceptions.SERVER_VERSION_NOT_SUPPORTED;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

abstract class PGMetaData {

  static class ColumnData {
    String tableSchemaName;
    String tableName;
    int relationId;
    int relationAttrNum;
    String columnName;
    Type type;
    int typeModifier;
    int typeLength;
    Boolean nullable;
    String defaultValue;
    String description;
    Type baseType;
  }

  static class AttributeData {
    String typeSchemaName;
    String typeName;
    int relationId;
    int relationAttrNum;
    String attributeName;
    Type type;
    int typeModifier;
    int typeLength;
    Boolean nullable;
    String defaultValue;
    String description;
  }

  PGDirectConnection connection;

  PGMetaData(PGDirectConnection connection) {
    this.connection = connection;
  }

  public PGDirectConnection getConnection() throws SQLException {
    return connection;
  }

  int execForInteger(String query) throws SQLException {

    String res = getConnection().executeForString(query);
    if (res == null) {
      throw SERVER_VERSION_NOT_SUPPORTED;
    }

    try {
      return Integer.parseInt(res);
    }
    catch (NumberFormatException e) {
      throw SERVER_VERSION_NOT_SUPPORTED;
    }

  }

  PGResultSet execForResultSet(String sql, Object... params) throws SQLException {

    return execForResultSet(sql, Arrays.asList(params));
  }

  PGResultSet execForResultSet(String sql, List<Object> params) throws SQLException {

    PGPreparedStatement ps = getConnection().prepareStatement(sql);
    ps.closeOnCompletion();

    for (int c = 0; c < params.size(); ++c) {
      ps.setObject(c + 1, params.get(c));
    }

    return ps.executeQuery();
  }

  PGResultSet createResultSet(ResultField[] resultFields, List<Object[]> results) throws SQLException {

    RowDataSet rows = new RowDataSet(results.size());
    for (Object[] resultValues : results) {
      try {
        rows.add(BufferRowData.encode(getConnection(), resultFields, resultValues));
      }
      catch (IOException e) {
        throw new PGSQLSimpleException("Error encoding row value", e);
      }
    }

    PGStatement stmt = getConnection().createStatement();
    stmt.closeOnCompletion();
    return stmt.createResultSet(resultFields, rows, true, getConnection().getTypeMap());
  }

  List<ColumnData> getColumnData(String sql, List<Object> params) throws SQLException {

    Registry registry = connection.getRegistry();

    List<ColumnData> columnsData = new ArrayList<>();

    try (ResultSet rs = execForResultSet(sql, params)) {

      while (rs.next()) {

        ColumnData columnData = new ColumnData();

        columnData.tableSchemaName = rs.getString("nspname");
        columnData.tableName = rs.getString("relname");
        columnData.relationId = rs.getInt("attrelid");
        columnData.relationAttrNum = rs.getInt("attnum");
        columnData.columnName = rs.getString("attname");
        columnData.type = registry.loadType(rs.getInt("atttypid"));
        columnData.typeModifier = rs.getInt("atttypmod");
        columnData.typeLength = rs.getInt("attlen");
        columnData.nullable = !rs.getBoolean("attnotnull");
        columnData.nullable = rs.wasNull() ? null : columnData.nullable;
        columnData.defaultValue = rs.getString("adsrc");
        columnData.description = rs.getString("description");
        columnData.baseType = registry.loadType(rs.getInt("typbasetype"));

        columnsData.add(columnData);
      }

    }
    catch (IOException e) {
      throw makeSQLException(e);
    }

    return columnsData;
  }

  StringBuilder getColumnSQL(CharSequence extraWhereConditions) {
    return new StringBuilder(
            "   SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,a.attrelid," +
            "     row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype " +
            "   FROM pg_catalog.pg_namespace n " +
            "   JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) " +
            "   JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid) " +
            "   JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) " +
            "   LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) " +
            "   LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) " +
            "   LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') " +
            "   LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') " +
            "   WHERE NOT a.attisdropped ")
        .append(extraWhereConditions);
  }

}
