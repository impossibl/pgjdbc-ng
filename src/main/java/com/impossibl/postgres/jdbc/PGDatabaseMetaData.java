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

import com.impossibl.postgres.api.data.ACLItem;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ResultField.Format;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.guava.Joiner;

import static com.impossibl.postgres.jdbc.Exceptions.NOT_IMPLEMENTED;
import static com.impossibl.postgres.jdbc.Exceptions.SERVER_VERSION_NOT_SUPPORTED;
import static com.impossibl.postgres.jdbc.Exceptions.UNWRAP_ERROR;
import static com.impossibl.postgres.system.Settings.CREDENTIALS_USERNAME;
import static com.impossibl.postgres.system.Settings.DATABASE_URL;
import static com.impossibl.postgres.utils.guava.Strings.isNullOrEmpty;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PseudoColumnUsage;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

class PGDatabaseMetaData implements DatabaseMetaData {

  private static final String EXTRA_KEYWORDS =
      "abort,acl,add,aggregate,append,archive," +
      "arch_store,backward,binary,boolean,change,cluster," +
      "copy,database,delimiter,delimiters,do,extend," +
      "explain,forward,heavy,index,inherits,isnull," +
      "light,listen,load,merge,nothing,notify," +
      "notnull,oids,purge,rename,replace,retrieve," +
      "returns,rule,recipe,setof,stdin,stdout,store," +
      "vacuum,verbose,version";

  PGConnectionImpl connection;
  int maxNameLength;
  int maxIndexKeys;

  PGDatabaseMetaData(PGConnectionImpl connection) {
    this.connection = connection;
  }

  private int execForInteger(String query) throws SQLException {

    String res = connection.executeForString(query, false);
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

  private PGResultSet execForResultSet(String sql, Object... params) throws SQLException {

    return execForResultSet(sql, Arrays.asList(params));
  }

  private PGResultSet execForResultSet(String sql, List<Object> params) throws SQLException {

    PGPreparedStatement ps = connection.prepareStatement(sql);
    ps.closeOnCompletion();

    for (int c = 0; c < params.size(); ++c) {
      ps.setObject(c + 1, params.get(c));
    }

    return ps.executeQuery();
  }

  private PGResultSet createResultSet(List<ResultField> resultFields, List<Object[]> results) throws SQLException {

    PGStatement stmt = connection.createStatement();
    stmt.closeOnCompletion();
    return stmt.createResultSet(resultFields, results);
  }

  private int getMaxNameLength() throws SQLException {

    if (maxNameLength == 0) {

      maxNameLength =
          execForInteger("SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'");

    }

    return maxNameLength;
  }

  protected int getMaxIndexKeys() throws SQLException {

    if (maxIndexKeys == 0) {

      maxIndexKeys = execForInteger("SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'");

    }

    return maxIndexKeys;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (!iface.isAssignableFrom(getClass())) {
      throw UNWRAP_ERROR;
    }

    return iface.cast(this);
  }

  @Override
  public boolean isWrapperFor (Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    Object val = connection.getSetting(DATABASE_URL);
    if (val == null)
      throw new SQLException("invalid connection");
    return val.toString();
  }

  @Override
  public String getUserName() throws SQLException {
    Object val = connection.getSetting(CREDENTIALS_USERNAME);
    if (val == null)
      throw new SQLException("invalid connection");
    return val.toString();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "PostgreSQL";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return connection.getServerVersion().toString();
  }

  @Override
  public String getDriverName() throws SQLException {
    return "PostgreSQL JDBC - NG";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return PGDriver.VERSION.toString();
  }

  @Override
  public int getDriverMajorVersion() {
    return PGDriver.VERSION.getMajor();
  }

  @Override
  public int getDriverMinorVersion() {
    return PGDriver.VERSION.getMinor();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return EXTRA_KEYWORDS;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return Joiner.on(',').join(SQLTextEscapeFunctions.ALL_NUMERIC);
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return Joiner.on(',').join(SQLTextEscapeFunctions.ALL_STRING);
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return Joiner.on(',').join(SQLTextEscapeFunctions.ALL_SYSTEM);
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return Joiner.on(',').join(SQLTextEscapeFunctions.ALL_DATE_TIME);
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "schema";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "function";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return true;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return getMaxIndexKeys();
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 1600;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 0; //Larger than int (1.6 TB)
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return getMaxNameLength();
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {

    switch(level) {
      case Connection.TRANSACTION_NONE:
      case Connection.TRANSACTION_READ_UNCOMMITTED:
      case Connection.TRANSACTION_READ_COMMITTED:
      case Connection.TRANSACTION_REPEATABLE_READ:
      case Connection.TRANSACTION_SERIALIZABLE:
        return true;

      default:
        return false;
    }

  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT NULL AS PROCEDURE_CAT, n.nspname AS PROCEDURE_SCHEM, p.proname AS PROCEDURE_NAME, NULL, NULL, NULL, " +
        " d.description AS REMARKS, " +  java.sql.DatabaseMetaData.procedureReturnsResult + " AS PROCEDURE_TYPE, p.proname || '_' || p.oid AS SPECIFIC_NAME " +
        " FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc p " +
        " LEFT JOIN pg_catalog.pg_description d ON (p.oid=d.objoid) " +
        " LEFT JOIN pg_catalog.pg_class c ON (d.classoid=c.oid AND c.relname='pg_proc') " +
        " LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace=pn.oid AND pn.nspname='pg_catalog') " +
        " WHERE p.pronamespace=n.oid");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }
    if (!isNullOrEmpty(procedureNamePattern)) {
      sql.append(" AND p.proname LIKE ?");
      params.add(procedureNamePattern);
    }

    sql.append(" ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME");

    return execForResultSet(sql.toString(), params);
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {

    Registry reg = connection.getRegistry();

    ResultField[] resultFields = new ResultField[20];
    List<Object[]> results = new ArrayList<>();

    resultFields[0] =   new ResultField("PROCEDURE_CAT",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[1] =   new ResultField("PROCEDURE_SCHEM",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[2] =   new ResultField("PROCEDURE_NAME",     0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[3] =   new ResultField("COLUMN_NAME",        0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[4] =   new ResultField("COLUMN_TYPE",        0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[5] =   new ResultField("DATA_TYPE",          0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[6] =   new ResultField("TYPE_NAME",          0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[7] =   new ResultField("PRECISION",          0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[8] =   new ResultField("LENGTH",             0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[9] =   new ResultField("SCALE",              0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[10] =  new ResultField("RADIX",              0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[11] =  new ResultField("NULLABLE",           0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[12] =  new ResultField("REMARKS",            0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[13] =  new ResultField("COLUMN_DEF",         0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[14] =  new ResultField("SQL_DATA_TYPE",      0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[15] =  new ResultField("SQL_DATETIME_SUB",   0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[16] =  new ResultField("CHAR_OCTECT_LENGTH", 0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[17] =  new ResultField("ORDINAL_POSITION",   0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[18] =  new ResultField("IS_NULLABLE",        0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[19] =  new ResultField("SPECIFIC_NAME",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT n.nspname,p.proname,p.prorettype,p.proargtypes, t.typtype,t.typrelid, p.proargnames, " +
        " p.proargmodes, p.proallargtypes, p.oid " +
        " FROM pg_catalog.pg_proc p, pg_catalog.pg_namespace n, pg_catalog.pg_type t " +
        " WHERE p.pronamespace=n.oid AND p.prorettype=t.oid ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }
    if (!isNullOrEmpty(procedureNamePattern)) {
      sql.append(" AND p.proname LIKE ?");
      params.add(procedureNamePattern);
    }

    sql.append(" ORDER BY n.nspname, p.proname, p.oid::text ");

    try (PGResultSet rs = execForResultSet(sql.toString(), params)) {
      while (rs.next()) {

        String schema = rs.getString("nspname");
        String procedureName = rs.getString("proname");
        String specificName = rs.getString("proname") + "_" + rs.getString("oid");
        Type returnType = reg.loadType(rs.getInt("prorettype"));
        String returnTypeType = rs.getString("typtype");
        int returnTypeRelId = rs.getInt("typrelid");

        Integer[] argTypeIds = rs.getObject("proargtypes", Integer[].class);
        String[] argNames = rs.getObject("proargnames", String[].class);
        String[] argModes = rs.getObject("proargmodes", String[].class);
        Integer[] allArgTypeIds = rs.getObject("proallargtypes", Integer[].class);

        int numArgs = allArgTypeIds != null ? allArgTypeIds.length : argTypeIds.length;

        // decide if we are returning a single column result.
        if (returnTypeType.equals("b") ||
            returnTypeType.equals("d") ||
            (returnTypeType.equals("p") && argModes == null)) {

          Object[] row = new Object[resultFields.length];
          row[0] = null;
          row[1] = schema;
          row[2] = procedureName;
          row[3] = "returnValue";
          row[4] = DatabaseMetaData.procedureColumnReturn;
          row[5] = SQLTypeMetaData.getSQLType(returnType);
          row[6] = SQLTypeMetaData.getTypeName(returnType, null, 0);
          row[7] = null;
          row[8] = null;
          row[9] = null;
          row[10] = null;
          row[11] = DatabaseMetaData.procedureNullableUnknown;
          row[12] = null;
          row[17] = 0;
          row[18] = "";
          row[19] = specificName;

          results.add(row);
        }

        // Add a row for each argument.
        for (int i = 0; i < numArgs; i++) {

          Object[] row = new Object[resultFields.length];
          row[0] = null;
          row[1] = schema;
          row[2] = procedureName;

          if (argNames != null) {
            row[3] = argNames[i];
          }
          else {
            row[3] = "$" + (i + 1);
          }

          int columnMode = DatabaseMetaData.procedureColumnIn;
          if (argModes != null) {

            if (argModes[i].equals("o")) {
              columnMode = DatabaseMetaData.procedureColumnOut;
            }
            else if (argModes[i].equals("b")) {
              columnMode = DatabaseMetaData.procedureColumnInOut;
            }
          }

          row[4] = columnMode;

          Type argType;
          if (allArgTypeIds != null) {
            argType = reg.loadType(allArgTypeIds[i].intValue());
          }
          else {
            argType = reg.loadType(argTypeIds[i].intValue());
          }

          row[5] = SQLTypeMetaData.getSQLType(argType);
          row[6] = argType.getJavaType(argType.getPreferredFormat(), connection.getTypeMap()).getName();
          row[7] = null;
          row[8] = null;
          row[9] = null;
          row[10] = null;
          row[11] = DatabaseMetaData.procedureNullableUnknown;
          row[12] = null;
          row[17] = i + 1;
          row[18] = "";
          row[19] = specificName;

          results.add(row);
        }

        // if we are returning a multi-column result.
        if (returnTypeType.equals("c") ||
            (returnTypeType.equals("p") && argModes != null && returnTypeRelId != 0)) {

          String columnsql = "SELECT a.attname,a.atttypid FROM pg_catalog.pg_attribute a WHERE a.attrelid = " + returnTypeRelId + " AND a.attnum > 0 ORDER BY a.attnum ";
          try (ResultSet columnrs = connection.createStatement().executeQuery(columnsql)) {
            while (columnrs.next()) {
              Type columnType = reg.loadType(columnrs.getInt("atttypid"));

              Object[] row = new Object[resultFields.length];
              row[0] = null;
              row[1] = schema;
              row[2] = procedureName;
              row[3] = columnrs.getString("attname");
              row[4] = DatabaseMetaData.procedureColumnResult;
              row[5] = SQLTypeMetaData.getSQLType(columnType);
              row[6] = columnType.getJavaType(columnType.getPreferredFormat(), connection.getTypeMap()).getName();
              row[7] = null;
              row[8] = null;
              row[9] = null;
              row[10] = null;
              row[11] = DatabaseMetaData.procedureNullableUnknown;
              row[12] = null;
              row[17] = 0;
              row[18] = "";
              row[19] = specificName;

              results.add(row);
            }
          }
        }

      }
    }

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME, " +
        " CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema' " +
        " WHEN true THEN CASE " +
        " WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind " +
        "  WHEN 'r' THEN 'SYSTEM TABLE' " +
        "  WHEN 'v' THEN 'SYSTEM VIEW' " +
        "  WHEN 'i' THEN 'SYSTEM INDEX' " +
        "  ELSE NULL " +
        "  END " +
        " WHEN n.nspname = 'pg_toast' THEN CASE c.relkind " +
        "  WHEN 'r' THEN 'SYSTEM TOAST TABLE' " +
        "  WHEN 'i' THEN 'SYSTEM TOAST INDEX' " +
        "  ELSE NULL " +
        "  END " +
        " ELSE CASE c.relkind " +
        "  WHEN 'r' THEN 'TEMPORARY TABLE' " +
        "  WHEN 'i' THEN 'TEMPORARY INDEX' " +
        "  WHEN 'S' THEN 'TEMPORARY SEQUENCE' " +
        "  WHEN 'v' THEN 'TEMPORARY VIEW' " +
        "  ELSE NULL " +
        "  END " +
        " END " +
        " WHEN false THEN CASE c.relkind " +
        " WHEN 'r' THEN 'TABLE' " +
        " WHEN 'i' THEN 'INDEX' " +
        " WHEN 'S' THEN 'SEQUENCE' " +
        " WHEN 'v' THEN 'VIEW' " +
        " WHEN 'c' THEN 'TYPE' " +
        " WHEN 'f' THEN 'FOREIGN TABLE' " +
        " ELSE NULL " +
        " END " +
        " ELSE NULL " +
        " END " +
        " AS TABLE_TYPE, d.description AS REMARKS " +
        " FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c " +
        " LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0) " +
        " LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class') " +
        " LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog') " +
        " WHERE c.relnamespace = n.oid ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }

    if (!isNullOrEmpty(tableNamePattern)) {
      sql.append(" AND c.relname LIKE ?");
      params.add(tableNamePattern);
    }

    if (types != null) {
      sql.append(" AND (false ");
      for (int i = 0; i < types.length; i++) {
        String clause = tableTypeClauses.get(types[i]);
        if (clause != null) {
          sql.append(" OR ( " + clause + " ) ");
        }
      }
      sql.append(") ");
    }

    sql.append(" ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME ");

    return execForResultSet(sql.toString(), params);
  }

  private static final Map<String, String> tableTypeClauses;
  static {
    tableTypeClauses = new HashMap<>();
    tableTypeClauses.put("TABLE", "c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    tableTypeClauses.put("VIEW", "c.relkind = 'v' AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'");
    tableTypeClauses.put("INDEX", "c.relkind = 'i' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    tableTypeClauses.put("SEQUENCE", "c.relkind = 'S'");
    tableTypeClauses.put("TYPE", "c.relkind = 'c' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'");
    tableTypeClauses.put("SYSTEM TABLE", "c.relkind = 'r' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema')");
    tableTypeClauses.put("SYSTEM TOAST TABLE", "c.relkind = 'r' AND n.nspname = 'pg_toast'");
    tableTypeClauses.put("SYSTEM TOAST INDEX", "c.relkind = 'i' AND n.nspname = 'pg_toast'");
    tableTypeClauses.put("SYSTEM VIEW", "c.relkind = 'v' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    tableTypeClauses.put("SYSTEM INDEX", "c.relkind = 'i' AND (n.nspname = 'pg_catalog' OR n.nspname = 'information_schema') ");
    tableTypeClauses.put("TEMPORARY TABLE", "c.relkind = 'r' AND n.nspname ~ '^pg_temp_' ");
    tableTypeClauses.put("TEMPORARY INDEX", "c.relkind = 'i' AND n.nspname ~ '^pg_temp_' ");
    tableTypeClauses.put("TEMPORARY VIEW", "c.relkind = 'v' AND n.nspname ~ '^pg_temp_' ");
    tableTypeClauses.put("TEMPORARY SEQUENCE", "c.relkind = 'S' AND n.nspname ~ '^pg_temp_' ");
    tableTypeClauses.put("FOREIGN TABLE", "c.relkind = 'f'");
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {

    //Select all schemas the current user has access to including temp and toast schemas

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT nspname AS TABLE_SCHEM,  NULL AS TABLE_CATALOG" +
        " FROM pg_catalog.pg_namespace " +
        " WHERE " +
        " (" +
        " nspname <> 'pg_toast'" +
        " AND" +
        " (nspname !~ '^pg_temp_\\d+' OR nspname = (pg_catalog.current_schemas(true))[1])" +
        " AND" +
        " (nspname !~ '^pg_toast_temp_' OR nspname = replace((pg_catalog.current_schemas(true))[1], 'pg_temp_', 'pg_toast_temp_'))" +
        " )");

    if (schemaPattern != null) {
      sql.append(" AND nspname LIKE ?");
      params.add(schemaPattern);
    }

    sql.append(" ORDER BY TABLE_SCHEM");

    return execForResultSet(sql.toString(), params);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {

    ResultField[] resultFields = new ResultField[1];
    resultFields[0] = new ResultField("TABLE_CAT", 0, (short)0, connection.getRegistry().loadType("text"), (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();
    results.add(new Object[] {connection.getCatalog()});

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {

    ResultField[] resultFields = new ResultField[1];
    resultFields[0] = new ResultField("TABLE_TYPE", 0, (short)0, connection.getRegistry().loadType("text"), (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();

    for (String tableType : tableTypeClauses.keySet()) {
      results.add(new Object[] {tableType});
    }


    return createResultSet(Arrays.asList(resultFields), results);
  }

  static class ColumnData {
    String tableSchemaName;
    String tableName;
    CompositeType relationType;
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

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {

    Registry registry = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT * FROM (" +
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
        "   WHERE a.attnum > 0 AND NOT a.attisdropped ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }

    if (!isNullOrEmpty(tableNamePattern)) {
      sql.append(" AND c.relname LIKE ?");
      params.add(tableNamePattern);
    }

    sql.append(") c");

    if (!isNullOrEmpty(columnNamePattern)) {
      sql.append(" WHERE attname LIKE ?");
      params.add(columnNamePattern);
    }

    sql.append(" ORDER BY nspname,c.relname,attnum ");

    //Build list of column fields and data

    List<ColumnData> columnsData  = new ArrayList<>();

    try (ResultSet rs = execForResultSet(sql.toString(), params)) {

      while (rs.next()) {

        ColumnData columnData = new ColumnData();

        columnData.tableSchemaName = rs.getString("nspname");
        columnData.tableName = rs.getString("relname");
        columnData.relationType = registry.loadRelationType(rs.getInt("attrelid"));
        columnData.relationAttrNum = rs.getInt("attnum");
        columnData.columnName = rs.getString("attname");
        columnData.type = registry.loadType(rs.getInt("atttypid"));
        columnData.typeModifier = rs.getInt("atttypmod");
        columnData.typeLength = rs.getInt("attlen");
        columnData.nullable = !rs.getBoolean("attnotnull");
        columnData.defaultValue = rs.getString("adsrc");
        columnData.description = rs.getString("description");
        columnData.baseType = registry.loadType(rs.getInt("typbasetype"));

        columnsData.add(columnData);
      }

    }

    //Build result set (manually)

    ResultField[] resultFields = new ResultField[24];
    resultFields[0] = new ResultField("TABLE_CAT",          0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[1] = new ResultField("TABLE_SCHEM",        0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[2] = new ResultField("TABLE_NAME",         0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[3] = new ResultField("COLUMN_NAME",        0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[4] = new ResultField("DATA_TYPE",          0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[5] = new ResultField("TYPE_NAME",          0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[6] = new ResultField("COLUMN_SIZE",        0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[7] = new ResultField("BUFFER_LENGTH",      0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[8] = new ResultField("DECIMAL_DIGITS",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[9] = new ResultField("NUM_PREC_RADIX",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[10] = new ResultField("NULLABLE",          0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[11] = new ResultField("REMARKS",           0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[12] = new ResultField("COLUMN_DEF",        0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[13] = new ResultField("SQL_DATA_TYPE",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[14] = new ResultField("SQL_DATETIME_SUB",  0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[15] = new ResultField("CHAR_OCTET_LENGTH", 0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[16] = new ResultField("ORDINAL_POSITION",  0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[17] = new ResultField("IS_NULLABLE",       0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[18] = new ResultField("SCOPE_CATLOG",      0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[19] = new ResultField("SCOPE_SCHEMA",      0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[20] = new ResultField("SCOPE_TABLE",       0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[21] = new ResultField("SOURCE_DATA_TYPE",  0, (short)0, registry.loadType("int2"),   (short)0, 0, Format.Binary);
    resultFields[22] = new ResultField("IS_AUTOINCREMENT",  0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[23] = new ResultField("IS_GENERATEDCOLUMN", 0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();

    for (int c = 0; c < columnsData.size(); ++c) {

      ColumnData columnData = columnsData.get(c);

      Object[] row = new Object[resultFields.length];

      row[0] = null;
      row[1] = columnData.tableSchemaName;
      row[2] = columnData.tableName;
      row[3] = columnData.columnName;
      row[4] = SQLTypeMetaData.getSQLType(columnData.type);
      row[5] = SQLTypeMetaData.getTypeName(columnData.type, columnData.relationType, columnData.relationAttrNum);

      int size = SQLTypeMetaData.getPrecision(columnData.type, columnData.typeLength, columnData.typeModifier);
      if (size == 0) {
        size = SQLTypeMetaData.getDisplaySize(columnData.type, columnData.typeLength, columnData.typeModifier);
      }

      row[6] = size;
      row[7] = null;
      row[8] = SQLTypeMetaData.getScale(columnData.type, columnData.typeLength, columnData.typeModifier);

      row[9] = SQLTypeMetaData.getPrecisionRadix(columnData.type);
      row[10] = SQLTypeMetaData.isNullable(columnData.type, columnData.relationType, columnData.relationAttrNum);
      row[11] = columnData.description;
      row[12] = columnData.defaultValue;
      row[13] = null;
      row[14] = null;
      row[15] = columnData.typeLength;
      row[16] = columnData.relationAttrNum;

      String nullable = null;
      switch((int)row[10]) {
        case columnNoNulls:
          nullable = "NO";
          break;
        case columnNullable:
          nullable = "YES";
          break;
        default:
          nullable = "";
          break;
      }

      row[17] = nullable;
      row[18] = null;
      row[19] = null;
      row[20] = null;
      row[21] = columnData.baseType != null ? SQLTypeMetaData.getSQLType(columnData.baseType) : null;
      row[22] = SQLTypeMetaData.isAutoIncrement(columnData.type, columnData.relationType, columnData.relationAttrNum) ? "YES" : "NO";
      row[23] = columnData.relationType != null ? "YES" : "NO";

      results.add(row);
    }

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schemaPattern, String table, String columnNamePattern) throws SQLException {

    if (table == null) {
      table = "%";
    }

    if (columnNamePattern == null) {
      columnNamePattern = "%";
    }

    Registry reg = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT" +
        " n.nspname,c.relname,r.rolname,c.relacl,a.attacl,a.attname " +
        "FROM" +
        " pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_roles r, pg_catalog.pg_attribute a " +
        "WHERE " +
        " c.relnamespace = n.oid AND c.relowner = r.oid AND c.oid = a.attrelid AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname = ?");
      params.add(schemaPattern);
    }

    sql.append(" AND c.relname = ?");
    params.add(table);

    if (!isNullOrEmpty(columnNamePattern)) {
      sql.append(" AND a.attname LIKE ?");
      params.add(columnNamePattern);
    }

    sql.append(" ORDER BY attname");


    ResultField[] fields = new ResultField[8];
    fields[0] = new ResultField("TABLE_CAT",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[1] = new ResultField("TABLE_SCHEM",  0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[2] = new ResultField("TABLE_NAME",   0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[3] = new ResultField("COLUMN_NAME",  0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[4] = new ResultField("GRANTOR",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[5] = new ResultField("GRANTEE",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[6] = new ResultField("PRIVILEGE",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[7] = new ResultField("IS_GRANTABLE", 0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();

    try (ResultSet rs = execForResultSet(sql.toString(), params)) {

      while (rs.next()) {

        String schemaName = rs.getString("nspname");
        String tableName = rs.getString("relname");
        String column = rs.getString("attname");
        String owner = rs.getString("rolname");

        Map<String, Map<String, List<String[]>>> privileges = new HashMap<>();
        mapACLPrivileges(owner, rs.getObject("relacl", ACLItem[].class), privileges);
        mapACLPrivileges(owner, rs.getObject("attacl", ACLItem[].class), privileges);

        //Gather list privelege names
        String[] privNames = new String[privileges.size()];
        Iterator<String> e = privileges.keySet().iterator();
        int i = 0;
        while (e.hasNext()) {
          privNames[i++] = e.next();
        }

        Arrays.sort(privNames);

        for (String privName : privNames) {

          Map<String, List<String[]>> grantees = privileges.get(privName);

          //Gather list of grantee users
          String[] granteeUsers = new String[grantees.size()];
          Iterator<String> g = grantees.keySet().iterator();
          int k = 0;
          while (g.hasNext()) {
            granteeUsers[k++] = g.next();
          }

          //Add row for grantee

          for (int j = 0; j < grantees.size(); j++) {

            List<String[]> grantors = grantees.get(granteeUsers[j]);
            String grantee = granteeUsers[j];

            for (int l = 0; l < grantors.size(); l++) {

              String[] grants = grantors.get(l);

              String grantor = isNullOrEmpty(grants[0]) ? owner : grants[0];
              String grantable = owner.equals(grantee) ? "YES" : grants[1];

              Object[] row = new Object[8];
              row[0] = null;
              row[1] = schemaName;
              row[2] = tableName;
              row[3] = column;
              row[4] = grantor;
              row[5] = grantee;
              row[6] = privName;
              row[7] = grantable;

              results.add(row);
            }

          }

        }

      }

    }

    return createResultSet(Arrays.asList(fields), results);
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {

    Registry reg = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT" +
        " n.nspname,c.relname,r.rolname,c.relacl " +
        "FROM" +
        " pg_catalog.pg_namespace n, pg_catalog.pg_class c, pg_catalog.pg_roles r " +
        "WHERE" +
        " c.relnamespace = n.oid AND c.relowner = r.oid AND c.relkind = 'r' ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }

    if (!isNullOrEmpty(tableNamePattern)) {
      sql.append(" AND c.relname LIKE ?");
      params.add(tableNamePattern);
    }

    sql.append(" ORDER BY nspname, relname ");


    ResultField[] fields = new ResultField[7];
    fields[0] = new ResultField("TABLE_CAT",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[1] = new ResultField("TABLE_SCHEM",  0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[2] = new ResultField("TABLE_NAME",   0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[3] = new ResultField("GRANTOR",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[4] = new ResultField("GRANTEE",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[5] = new ResultField("PRIVILEGE",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    fields[6] = new ResultField("IS_GRANTABLE", 0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();

    try (ResultSet rs = execForResultSet(sql.toString(), params)) {

      while (rs.next()) {

        String schema = rs.getString("nspname");
        String table = rs.getString("relname");
        String owner = rs.getString("rolname");

        Map<String, Map<String, List<String[]>>> privileges = new HashMap<>();
        mapACLPrivileges(owner, rs.getObject("relacl", ACLItem[].class), privileges);

        String[] privNames = new String[privileges.size()];
        Iterator<String> e = privileges.keySet().iterator();
        int i = 0;
        while (e.hasNext()) {
          privNames[i++] = e.next();
        }

        Arrays.sort(privNames);

        for (String privName : privNames) {

          Map<String, List<String[]>> grantees = privileges.get(privName);

          //Gather list of grantee users
          String[] granteeUsers = new String[grantees.size()];
          Iterator<String> g = grantees.keySet().iterator();
          int k = 0;
          while (g.hasNext()) {
            granteeUsers[k++] = g.next();
          }

          for (int j = 0; j < granteeUsers.length; j++) {

            List<String[]> grantors = grantees.get(granteeUsers[j]);
            String grantee = granteeUsers[j];

            for (int l = 0; l < grantors.size(); l++) {

              String[] grants = grantors.get(l);

              String grantor = isNullOrEmpty(grants[0]) ? owner : grants[0];
              String grantable = owner.equals(grantee) ? "YES" : grants[1];

              Object[] row = new Object[7];
              row[0] = null;
              row[1] = schema;
              row[2] = table;
              row[3] = grantor;
              row[4] = grantee;
              row[5] = privName;
              row[6] = grantable;

              results.add(row);

            }
          }
        }
      }
    }

    return createResultSet(Arrays.asList(fields), results);
  }

  private void mapACLPrivileges(String owner, ACLItem[] aclItems, Map<String, Map<String, List<String[]>>> privileges) {

    if (aclItems == null) {
      // Null is shortcut for owner having full privileges
      ACLItem fullPrivs = new ACLItem(owner, "arwdDxt", owner);
      aclItems = new ACLItem[] {fullPrivs};
    }

    for (ACLItem aclItem : aclItems) {

      if (aclItem == null) {
        continue;
      }

      for (int i = 0; i < aclItem.privileges.length(); i++) {

        char c = aclItem.privileges.charAt(i);
        if (c != '*') {

          String sqlpriv;
          String grantable;
          if (i < aclItem.privileges.length() - 1 && aclItem.privileges.charAt(i + 1) == '*') {
            grantable = "YES";
          }
          else {
            grantable = "NO";
          }

          switch(c) {
            case 'a':
              sqlpriv = "INSERT";
              break;
            case 'r':
              sqlpriv = "SELECT";
              break;
            case 'w':
              sqlpriv = "UPDATE";
              break;
            case 'd':
              sqlpriv = "DELETE";
              break;
            case 'D':
              sqlpriv = "TRUNCATE";
              break;
            case 'R':
              sqlpriv = "RULE";
              break;
            case 'x':
              sqlpriv = "REFERENCES";
              break;
            case 't':
              sqlpriv = "TRIGGER";
              break;
            case 'X':
              sqlpriv = "EXECUTE";
              break;
            case 'U':
              sqlpriv = "USAGE";
              break;
            case 'C':
              sqlpriv = "CREATE";
              break;
            case 'T':
              sqlpriv = "CREATE TEMP";
              break;
            default:
              sqlpriv = "UNKNOWN";
          }

          Map<String, List<String[]>> usersWithPermission = privileges.get(sqlpriv);
          if (usersWithPermission == null) {
            usersWithPermission = new HashMap<>();
            privileges.put(sqlpriv, usersWithPermission);
          }

          List<String[]> permissionByGrantor = usersWithPermission.get(aclItem.user);
          if (permissionByGrantor == null) {
            permissionByGrantor = new ArrayList<>();
            usersWithPermission.put(aclItem.user, permissionByGrantor);
          }

          permissionByGrantor.add(new String[] {aclItem.grantor, grantable});

        }

      }

    }

  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schemaPattern, String table, int scope, boolean nullable) throws SQLException {

    Registry reg = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT a.attname, a.atttypid, attlen, atttypmod " +
        "FROM pg_catalog.pg_class ct " +
        "  JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) " +
        "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) " +
        "  JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary, " +
        "             information_schema._pg_expandarray(i.indkey) AS keys " +
        "        FROM pg_catalog.pg_index i) i " +
        "    ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) ");

    if (schemaPattern != null) {
      sql.append(" WHERE n.nspname = ?");
      params.add(schemaPattern);
    }

    ResultField[] resultFields = new ResultField[8];
    List<Object[]> results = new ArrayList<>();

    resultFields[0] = new ResultField("SCOPE",          0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[1] = new ResultField("COLUMN_NAME",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[2] = new ResultField("DATA_TYPE",      0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[3] = new ResultField("TYPE_NAME",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[4] = new ResultField("COLUMN_SIZE",    0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[5] = new ResultField("BUFFER_LENGTH",  0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[6] = new ResultField("DECIMAL_DIGITS", 0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[7] = new ResultField("PSEUDO_COLUMN",  0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);

    try (ResultSet rs = execForResultSet(sql.toString(), params)) {
      while (rs.next()) {

        Object[] row = new Object[8];
        Type type = reg.loadType(rs.getInt("atttypid"));
        int typeLen = rs.getInt("attlen");
        int typeMod = rs.getInt("atttypmod");
        int decimalDigits = SQLTypeMetaData.getScale(type, typeLen, typeMod);
        int columnSize = SQLTypeMetaData.getPrecision(type, typeLen, typeMod);
        if (columnSize == 0) {
          columnSize = SQLTypeMetaData.getDisplaySize(type, typeLen, typeMod);
        }
        row[0] = scope;
        row[1] = rs.getString("attname");
        row[2] = SQLTypeMetaData.getSQLType(type);
        row[3] = SQLTypeMetaData.getTypeName(type, null, 0);
        row[4] = columnSize;
        row[5] = null; // unused
        row[6] = decimalDigits;
        row[7] = DatabaseMetaData.bestRowNotPseudo;

        results.add(row);
      }
    }

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schemaPattern, String table) throws SQLException {

    Registry reg = connection.getRegistry();

    ResultField[] resultFields = new ResultField[8];
    List<Object[]> results = new ArrayList<>();

    resultFields[0] = new ResultField("SCOPE",          0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[1] = new ResultField("COLUMN_NAME",    0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[2] = new ResultField("DATA_TYPE",      0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[3] = new ResultField("TYPE_NAME",      0, (short)0, reg.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[4] = new ResultField("COLUMN_SIZE",    0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[5] = new ResultField("BUFFER_LENGTH",  0, (short)0, reg.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[6] = new ResultField("DECIMAL_DIGITS", 0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[7] = new ResultField("PSEUDO_COLUMN",  0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);

    Object[] row = new Object[8];

    /* Postgresql does not have any column types that are
     * automatically updated like some databases' timestamp type.
     * We can't tell what rules or triggers might be doing, so we
     * are left with the system columns that change on an update.
     * An update may change all of the following system columns:
     * ctid, xmax, xmin, cmax, and cmin.  Depending on if we are
     * in a transaction and wether we roll it back or not the
     * only guaranteed change is to ctid. -KJ
     */

    Type type = reg.loadType("tid");

    row[0] = null;
    row[1] = "ctid";
    row[2] = SQLTypeMetaData.getSQLType(type);
    row[3] = SQLTypeMetaData.getTypeName(type, null, 0);
    row[4] = null;
    row[5] = null;
    row[6] = null;
    row[7] = DatabaseMetaData.versionColumnPseudo;

    results.add(row);

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schemaPattern, String table) throws SQLException {

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, " +
        "  ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, " +
        "  (i.keys).n AS KEY_SEQ, ci.relname AS PK_NAME " +
        "FROM pg_catalog.pg_class ct " +
        "  JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) " +
        "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) " +
        "  JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary, " +
        "             information_schema._pg_expandarray(i.indkey) AS keys " +
        "        FROM pg_catalog.pg_index i) i " +
        "    ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid) " +
        "  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) ");

    if (schemaPattern != null) {
      sql.append(" WHERE n.nspname = ?");
      params.add(schemaPattern);
    }

    sql.append(" AND ct.relname = ?");
    params.add(table);

    sql.append(" AND i.indisprimary ORDER BY table_name, pk_name, key_seq");

    return execForResultSet(sql.toString(), params);
  }

  protected ResultSet getImportedExportedKeys(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
  {
    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, " +
        "NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, " +
        "pos.n AS KEY_SEQ, " +
        "CASE con.confupdtype " +
        " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade +
        " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault +
        " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull +
        " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict +
        " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction +
        " ELSE NULL END AS UPDATE_RULE, " +
        "CASE con.confdeltype " +
        " WHEN 'c' THEN " + DatabaseMetaData.importedKeyCascade +
        " WHEN 'n' THEN " + DatabaseMetaData.importedKeySetNull +
        " WHEN 'd' THEN " + DatabaseMetaData.importedKeySetDefault +
        " WHEN 'r' THEN " + DatabaseMetaData.importedKeyRestrict +
        " WHEN 'a' THEN " + DatabaseMetaData.importedKeyNoAction +
        " ELSE NULL END AS DELETE_RULE, " +
        "con.conname AS FK_NAME, pkic.relname AS PK_NAME, " +
        "CASE " +
        " WHEN con.condeferrable AND con.condeferred THEN " + DatabaseMetaData.importedKeyInitiallyDeferred +
        " WHEN con.condeferrable THEN " + DatabaseMetaData.importedKeyInitiallyImmediate +
        " ELSE " + DatabaseMetaData.importedKeyNotDeferrable +
        " END AS DEFERRABILITY " +
        " FROM " +
        " pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka, " +
        " pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka, " +
        " pg_catalog.pg_constraint con, " +
        " pg_catalog.generate_series(1, " + getMaxIndexKeys() + ") pos(n), " +
        " pg_catalog.pg_depend dep, pg_catalog.pg_class pkic " +
        " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid " +
        " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid " +
        " AND con.contype = 'f' AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND pkic.relkind = 'i' AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid ");

    if (!isNullOrEmpty(primarySchema)) {
      sql.append(" AND pkn.nspname = ?");
      params.add(primarySchema);
    }
    if (!isNullOrEmpty(foreignSchema)) {
      sql.append(" AND fkn.nspname = ?");
      params.add(foreignSchema);
    }
    if (!isNullOrEmpty(primaryTable)) {
      sql.append(" AND pkc.relname = ?");
      params.add(primaryTable);
    }
    if (!isNullOrEmpty(foreignTable)) {
      sql.append(" AND fkc.relname = ?");
      params.add(foreignTable);
    }

    if (primaryTable != null) {
      sql.append(" ORDER BY fkn.nspname,fkc.relname,pos.n");
    }
    else {
      sql.append(" ORDER BY pkn.nspname,pkc.relname,pos.n");
    }

    return execForResultSet(sql.toString(), params);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schemaPattern, String table) throws SQLException {
    return getImportedExportedKeys(null, null, null, catalog, schemaPattern, table);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schemaPattern, String table) throws SQLException {
    return getImportedExportedKeys(catalog, schemaPattern, table, null, null, null);
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return getImportedExportedKeys(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {

    Registry registry = connection.getRegistry();

    ResultField[] resultFields = new ResultField[18];
    List<Object[]> results = new ArrayList<>();

    resultFields[0] =   new ResultField("TYPE_NAME",          0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[1] =   new ResultField("DATA_TYPE",          0, (short)0, registry.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[2] =   new ResultField("PRECISION",          0, (short)0, registry.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[3] =   new ResultField("LITERAL_PREFIX",     0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[4] =   new ResultField("LITERAL_SUFFIX",     0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[5] =   new ResultField("CREATE_PARAMS",      0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[6] =   new ResultField("NULLABLE",           0, (short)0, registry.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[7] =   new ResultField("CASE_SENSITIVE",     0, (short)0, registry.loadType("bool"), (short)0, 0, Format.Binary);
    resultFields[8] =   new ResultField("SEARCHABLE",         0, (short)0, registry.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[9] =   new ResultField("UNSIGNED_ATTRIBUTE", 0, (short)0, registry.loadType("bool"), (short)0, 0, Format.Binary);
    resultFields[10] =  new ResultField("FIXED_PREC_SCALE",   0, (short)0, registry.loadType("bool"), (short)0, 0, Format.Binary);
    resultFields[11] =  new ResultField("AUTO_INCREMENT",     0, (short)0, registry.loadType("bool"), (short)0, 0, Format.Binary);
    resultFields[12] =  new ResultField("LOCAL_TYPE_NAME",    0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[13] =  new ResultField("MINIMUM_SCALE",      0, (short)0, registry.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[14] =  new ResultField("MAXIMUM_SCALE",      0, (short)0, registry.loadType("int2"), (short)0, 0, Format.Binary);
    resultFields[15] =  new ResultField("SQL_DATA_TYPE",      0, (short)0, registry.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[16] =  new ResultField("SQL_DATETIME_SUB",   0, (short)0, registry.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[17] =  new ResultField("NUM_PREC_RADIX",     0, (short)0, registry.loadType("int4"), (short)0, 0, Format.Binary);

    String sql =
        "SELECT t.typname,t.oid FROM pg_catalog.pg_type t" +
        " JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) " +
        " WHERE n.nspname != 'pg_toast'";

    try (ResultSet rs = execForResultSet(sql)) {
      while (rs.next()) {

        Object[] row = new Object[18];
        int typeOid = rs.getInt(2);
        Type type = registry.loadType(typeOid);

        row[0] = SQLTypeMetaData.getTypeName(type, null, 0);
        row[1] = SQLTypeMetaData.getSQLType(type);
        row[2] = SQLTypeMetaData.getMaxPrecision(type);

        if (SQLTypeMetaData.requiresQuoting(type)) {
          row[3] = "\'";
          row[4] = "\'";
        }

        row[6] = SQLTypeMetaData.isNullable(type, null, 0);
        row[7] = SQLTypeMetaData.isCaseSensitive(type);
        row[8] = true;
        row[9] = !SQLTypeMetaData.isSigned(type);
        row[10] = SQLTypeMetaData.isCurrency(type);
        row[11] = SQLTypeMetaData.isAutoIncrement(type, null, 0);
        row[13] = SQLTypeMetaData.getMinScale(type);
        row[14] = SQLTypeMetaData.getMaxScale(type);
        row[15] = null; //Unused
        row[16] = null; //Unused
        row[17] = SQLTypeMetaData.getPrecisionRadix(type);

        results.add(row);

      }
    }

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schemaPattern, String table, boolean unique, boolean approximate) throws SQLException {

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, " +
        "  ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, " +
        "  NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, " +
        "  CASE i.indisclustered " +
        "    WHEN true THEN " + java.sql.DatabaseMetaData.tableIndexClustered +
        "    ELSE CASE am.amname " +
        "      WHEN 'hash' THEN " + java.sql.DatabaseMetaData.tableIndexHashed +
        "      ELSE " + java.sql.DatabaseMetaData.tableIndexOther +
        "    END " +
        "  END AS TYPE, " +
        "  (i.keys).n AS ORDINAL_POSITION, " +
        "  pg_catalog.pg_get_indexdef(ci.oid, (i.keys).n, false) AS COLUMN_NAME, " +
        "  CASE am.amcanorder " +
        "    WHEN true THEN CASE i.indoption[(i.keys).n - 1] & 1 " +
        "      WHEN 1 THEN 'D' " +
        "      ELSE 'A' " +
        "    END " +
        "    ELSE NULL " +
        "  END AS ASC_OR_DESC, " +
        "  ci.reltuples AS CARDINALITY, " +
        "  ci.relpages AS PAGES, " +
        "  pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION " +
        "FROM pg_catalog.pg_class ct " +
        "  JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) " +
        "  JOIN (SELECT i.indexrelid, i.indrelid, i.indoption, " +
        "          i.indisunique, i.indisclustered, i.indpred, " +
        "          i.indexprs, " +
        "          information_schema._pg_expandarray(i.indkey) AS keys " +
        "        FROM pg_catalog.pg_index i) i " +
        "    ON (ct.oid = i.indrelid) " +
        "  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) " +
        "  JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) " +
        "WHERE true ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname = ?");
      params.add(schemaPattern);
    }

    sql.append(" AND ct.relname = ?");
    params.add(table);

    if (unique) {
      sql.append(" AND i.indisunique ");
    }
    sql.append(" ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION ");

    return execForResultSet(sql.toString(), params);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return true;

      default:
        return false;
    }
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        //Support all types
        return true;

      default:
        return false;
    }
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {

    //TODO this depends of TX state & isolation level

    switch(type) {
      case ResultSet.TYPE_FORWARD_ONLY:
      case ResultSet.TYPE_SCROLL_INSENSITIVE:
        return false;

      default:
        return false;
    }
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {

    Registry reg = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT " +
        " NULL AS TYPE_CAT, n.nspname AS TYPE_SCHEM, t.typname AS TYPE_NAME, NULL AS CLASS_NAME, " +
        " CASE WHEN t.typtype='c' THEN " + java.sql.Types.STRUCT + " ELSE " + java.sql.Types.DISTINCT + " END AS DATA_TYPE, " +
        " pg_catalog.obj_description(t.oid, 'pg_type')  AS REMARKS, " +
        " typbasetype as BASE_TYPE_ID " +
        "FROM" +
        " pg_catalog.pg_type t, pg_catalog.pg_namespace n " +
        "WHERE" +
        " t.typnamespace = n.oid and n.nspname != 'pg_catalog' and n.nspname != 'pg_toast'");

    if (types != null) {

      sql.append(" AND (false ");

      for (int i = 0; i < types.length; i++) {
        switch(types[i]) {
          case java.sql.Types.STRUCT:
            sql.append(" or t.typtype = 'c'");
            break;
          case java.sql.Types.DISTINCT:
            sql.append(" or t.typtype = 'd'");
            break;
          default:
            break;
        }
      }

      sql.append(" ) ");
    }
    else {

      sql.append(" and t.typtype IN ('c','d') ");
    }

    // spec says that if typeNamePattern is a fully qualified name
    // then the schema and catalog are ignored

    if (typeNamePattern != null) {

      // search for qualifier
      int firstQualifier = typeNamePattern.indexOf('.');
      int secondQualifier = typeNamePattern.lastIndexOf('.');

      if (firstQualifier != -1) {

        // if one of them is -1 they both will be
        if (firstQualifier != secondQualifier) {
          // we have a catalog.schema.typename, ignore catalog
          schemaPattern = typeNamePattern.substring(firstQualifier + 1, secondQualifier);
        }
        else {
          // we just have a schema.typename
          schemaPattern = typeNamePattern.substring(0, firstQualifier);
        }
        // strip out just the typeName
        typeNamePattern = typeNamePattern.substring(secondQualifier + 1);
      }

      sql.append(" AND t.typname LIKE ?");
      params.add(typeNamePattern);
    }

    // schemaPattern may have been modified above
    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }

    sql.append(" ORDER BY data_type, type_schem, type_name");

    PGResultSet rs = execForResultSet(sql.toString(), params);

    ResultField[] fields = new ResultField[7];
    fields[0] = rs.getResultFields().get(0);
    fields[1] = rs.getResultFields().get(1);
    fields[2] = rs.getResultFields().get(2);
    fields[3] = rs.getResultFields().get(3);
    fields[4] = rs.getResultFields().get(4);
    fields[5] = rs.getResultFields().get(5);
    fields[6] = new ResultField("BASE_TYPE", 0, (short)0, reg.loadType("int2"), (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();
    while (rs.next()) {

      Object[] row = new Object[7];

      row[0] = rs.get(1);
      row[1] = rs.get(2);
      row[2] = rs.get(3);
      row[3] = rs.get(4);
      row[4] = rs.get(5);
      row[5] = rs.get(6);

      Type type = reg.loadType(rs.getInt(7));
      if (type != null) {
        row[6] = SQLTypeMetaData.getSQLType(type);
      }
      else {
        row[6] = null;
      }

      results.add(row);
    }

    return createResultSet(Arrays.asList(fields), results);
  }

  static class AttributeData {
    String typeSchemaName;
    String typeName;
    CompositeType relationType;
    int relationAttrNum;
    String attributeName;
    Type type;
    int typeModifier;
    int typeLength;
    Boolean nullable;
    String description;
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {

    Registry registry = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT * FROM (" +
        "   SELECT n.nspname,t.typname,a.attname,a.atttypid,a.attrelid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen," +
        "     row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, dsc.description" +
        "   FROM pg_catalog.pg_namespace n " +
        "   JOIN pg_catalog.pg_type t ON (t.typnamespace=n.oid) " +
        "   JOIN pg_catalog.pg_attribute a ON (a.attrelid=t.typrelid) " +
        "   LEFT JOIN pg_catalog.pg_description dsc ON (t.typrelid=dsc.objoid AND a.attnum = dsc.objsubid) " +
        "   LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class') " +
        "   LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') " +
        "   WHERE a.attnum > 0 AND NOT a.attisdropped AND t.typtype='c'");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }

    if (!isNullOrEmpty(typeNamePattern)) {
      sql.append(" AND t.typname LIKE ?");
      params.add(typeNamePattern);
    }

    sql.append(") c");

    if (!isNullOrEmpty(attributeNamePattern)) {
      sql.append(" WHERE attname LIKE ?");
      params.add(attributeNamePattern);
    }

    sql.append(" ORDER BY nspname,c.typname,attnum ");

    //Build list of column fields and data

    List<AttributeData> attrsData  = new ArrayList<>();

    try (ResultSet rs = execForResultSet(sql.toString(), params)) {

      while (rs.next()) {

        AttributeData attrData = new AttributeData();

        attrData.typeSchemaName = rs.getString("nspname");
        attrData.typeName = rs.getString("typname");
        attrData.relationType = registry.loadRelationType(rs.getInt("attrelid"));
        attrData.relationAttrNum = rs.getInt("attnum");
        attrData.attributeName = rs.getString("attname");
        attrData.type = registry.loadType(rs.getInt("atttypid"));
        attrData.typeModifier = rs.getInt("atttypmod");
        attrData.typeLength = rs.getInt("attlen");
        attrData.nullable = !rs.getBoolean("attnotnull");
        attrData.description = rs.getString("description");

        attrsData.add(attrData);
      }

    }

    //Build result set (manually)

    ResultField[] resultFields = new ResultField[21];
    resultFields[0] = new ResultField("TYPE_CAT",           0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[1] = new ResultField("TYPE_SCHEM",         0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[2] = new ResultField("TYPE_NAME",          0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[3] = new ResultField("ATTR_NAME",          0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[4] = new ResultField("DATA_TYPE",          0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[5] = new ResultField("ATTR_TYPE_NAME",     0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[6] = new ResultField("ATTR_SIZE",          0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[7] = new ResultField("DECIMAL_DIGITS",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[8] = new ResultField("NUM_PREC_RADIX",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[9] = new ResultField("NULLABLE",           0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[10] = new ResultField("REMARKS",           0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[11] = new ResultField("ATTR_DEF",          0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[12] = new ResultField("SQL_DATA_TYPE",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[13] = new ResultField("SQL_DATETIME_SUB",  0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[14] = new ResultField("CHAR_OCTET_LENGTH", 0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[15] = new ResultField("ORDINAL_POSITION",  0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[16] = new ResultField("IS_NULLABLE",       0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[17] = new ResultField("SCOPE_CATLOG",      0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[18] = new ResultField("SCOPE_SCHEMA",      0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[19] = new ResultField("SCOPE_TABLE",       0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[20] = new ResultField("SOURCE_DATA_TYPE",  0, (short)0, registry.loadType("int2"),   (short)0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();

    for (int c = 0; c < attrsData.size(); ++c) {

      AttributeData attrData = attrsData.get(c);

      Object[] row = new Object[resultFields.length];

      row[0] = null;
      row[1] = attrData.typeSchemaName;
      row[2] = attrData.typeName;
      row[3] = attrData.attributeName;
      row[4] = SQLTypeMetaData.getSQLType(attrData.type);
      row[5] = SQLTypeMetaData.getTypeName(attrData.type, attrData.relationType, attrData.relationAttrNum);

      int size = SQLTypeMetaData.getPrecision(attrData.type, attrData.typeLength, attrData.typeModifier);
      if (size == 0) {
        size = SQLTypeMetaData.getDisplaySize(attrData.type, attrData.typeLength, attrData.typeModifier);
      }

      row[6] = size;

      row[7] = SQLTypeMetaData.getScale(attrData.type, attrData.typeLength, attrData.typeModifier);
      row[8] = SQLTypeMetaData.getPrecisionRadix(attrData.type);
      row[9] = SQLTypeMetaData.isNullable(attrData.type, attrData.relationType, attrData.relationAttrNum);
      row[10] = attrData.description;
      row[11] = null;
      row[12] = null;
      row[13] = null;
      row[14] = attrData.typeLength;
      row[15] = attrData.relationAttrNum;

      String nullable = null;
      switch((int)row[9]) {
        case attributeNoNulls:
          nullable = "NO";
          break;
        case attributeNullable:
          nullable = "YES";
          break;
        default:
          nullable = "";
          break;
      }

      row[16] = nullable;
      row[17] = null;
      row[18] = null;
      row[19] = null;
      row[20] = attrData.type instanceof DomainType ? SQLTypeMetaData.getSQLType(attrData.type.unwrap()) : null;

      results.add(row);
    }

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return true;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {

    switch(holdability) {
      case ResultSet.CLOSE_CURSORS_AT_COMMIT:
      case ResultSet.HOLD_CURSORS_OVER_COMMIT:
        return true;

      default:
        return false;
    }
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return connection.getServerVersion().getMajor();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return connection.getServerVersion().getMinor();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 1;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    // TODO Not for now...
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_VALID_SESSION;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {

    Registry registry = connection.getRegistry();

    ResultField[] resultFields = new ResultField[18];
    List<Object[]> results = new ArrayList<>();

    resultFields[0] =   new ResultField("NAME",               0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[1] =   new ResultField("MAX_LEN",            0, (short)0, registry.loadType("int4"), (short)0, 0, Format.Binary);
    resultFields[2] =   new ResultField("DEFAULT_VALUE",      0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);
    resultFields[3] =   new ResultField("DESCRIPTION",        0, (short)0, registry.loadType("text"), (short)0, 0, Format.Binary);

    Object[] row = new Object[4];

    //ApplicationName
    row[0] = "ApplicationName";
    row[1] = -1;
    row[2] = "pgjdbc app";
    row[3] = "Name of application using the connection";
    results.add(row);

    return createResultSet(asList(resultFields), results);
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    throw NOT_IMPLEMENTED;
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {

    Registry registry = connection.getRegistry();

    StringBuilder sql = new StringBuilder();
    List<Object> params = new ArrayList<>();

    sql.append(
        "SELECT * FROM (" +
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
        "   WHERE a.attnum < 1 AND NOT a.attisdropped ");

    if (schemaPattern != null) {
      sql.append(" AND n.nspname LIKE ?");
      params.add(schemaPattern);
    }

    if (!isNullOrEmpty(tableNamePattern)) {
      sql.append(" AND c.relname LIKE ?");
      params.add(tableNamePattern);
    }

    sql.append(") c");

    if (!isNullOrEmpty(columnNamePattern)) {
      sql.append(" WHERE attname LIKE ?");
      params.add(columnNamePattern);
    }

    sql.append(" ORDER BY nspname,c.relname,attnum ");

    //Build list of column fields and data

    List<ColumnData> columnsData  = new ArrayList<>();

    try (ResultSet rs = execForResultSet(sql.toString(), params)) {

      while (rs.next()) {

        ColumnData columnData = new ColumnData();

        columnData.tableSchemaName = rs.getString("nspname");
        columnData.tableName = rs.getString("relname");
        columnData.relationType = registry.loadRelationType(rs.getInt("attrelid"));
        columnData.relationAttrNum = rs.getInt("attnum");
        columnData.columnName = rs.getString("attname");
        columnData.type = registry.loadType(rs.getInt("atttypid"));
        columnData.typeModifier = rs.getInt("atttypmod");
        columnData.typeLength = rs.getInt("attlen");
        columnData.nullable = !rs.getBoolean("attnotnull");
        columnData.defaultValue = rs.getString("adsrc");
        columnData.description = rs.getString("description");
        columnData.baseType = registry.loadType(rs.getInt("typbasetype"));

        columnsData.add(columnData);
      }

    }

    //Build result set (manually)

    ResultField[] resultFields = new ResultField[24];
    resultFields[0] = new ResultField("TABLE_CAT",          0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[1] = new ResultField("TABLE_SCHEM",        0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[2] = new ResultField("TABLE_NAME",         0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[3] = new ResultField("COLUMN_NAME",        0, (short)0, registry.loadType("text"),   (short)0, 0, Format.Binary);
    resultFields[4] = new ResultField("DATA_TYPE",          0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[5] = new ResultField("COLUMN_SIZE",        0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[6] = new ResultField("DECIMAL_DIGITS",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[7] = new ResultField("NUM_PREC_RADIX",     0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[8] = new ResultField("COLUMN_USAGE",      0, (short)0, registry.loadType("int4"),   (short)0, 0, Format.Binary);
    resultFields[9] = new ResultField("REMARKS", 0, (short) 0, registry.loadType("text"), (short) 0, 0, Format.Binary);
    resultFields[10] = new ResultField("CHAR_OCTET_LENGTH", 0, (short) 0, registry.loadType("int4"), (short) 0, 0, Format.Binary);
    resultFields[11] = new ResultField("IS_NULLABLE", 0, (short) 0, registry.loadType("text"), (short) 0, 0, Format.Binary);

    List<Object[]> results = new ArrayList<>();

    for (int c = 0; c < columnsData.size(); ++c) {

      ColumnData columnData = columnsData.get(c);

      Object[] row = new Object[resultFields.length];

      row[0] = null;
      row[1] = columnData.tableSchemaName;
      row[2] = columnData.tableName;
      row[3] = columnData.columnName;
      row[4] = SQLTypeMetaData.getSQLType(columnData.type);

      int size = SQLTypeMetaData.getPrecision(columnData.type, columnData.typeLength, columnData.typeModifier);
      if (size == 0) {
        size = SQLTypeMetaData.getDisplaySize(columnData.type, columnData.typeLength, columnData.typeModifier);
      }

      row[5] = size;
      row[6] = SQLTypeMetaData.getScale(columnData.type, columnData.typeLength, columnData.typeModifier);

      row[7] = SQLTypeMetaData.getPrecisionRadix(columnData.type);
      row[8] = PseudoColumnUsage.NO_USAGE_RESTRICTIONS.name();
      row[9] = columnData.description;
      row[10] = columnData.typeLength;

      String nullable = null;
      int isNullable = SQLTypeMetaData.isNullable(columnData.type, columnData.relationType, columnData.relationAttrNum);
      switch (isNullable) {
        case columnNoNulls:
          nullable = "NO";
          break;
        case columnNullable:
          nullable = "YES";
          break;
        default:
          nullable = "";
          break;
      }

      row[11] = nullable;

      results.add(row);
    }

    return createResultSet(Arrays.asList(resultFields), results);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

}
