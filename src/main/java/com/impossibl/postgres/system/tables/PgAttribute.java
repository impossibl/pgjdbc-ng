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
package com.impossibl.postgres.system.tables;

import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.system.Context;
import com.impossibl.postgres.system.Version;

import static com.impossibl.postgres.system.tables.Table.getFieldOfRow;

import java.io.IOException;


/**
 * Table for "pg_attribute"
 *
 * @author kdubb
 *
 */
public class PgAttribute implements Table<PgAttribute.Row> {

  public static class Row implements Table.Row {

    private int relationTypeId;
    private int relationId;
    private String name;
    private int typeId;
    private int typeModifier;
    private short length;
    private short number;
    private boolean nullable;
    private Boolean autoIncrement;
    private int numberOfDimensions;
    private boolean hasDefault;

    public Row(int relationTypeId, int relationId, String name, int typeId, int typeModifier, short length, short number, boolean nullable, Boolean autoIncrement, int numberOfDimensions, boolean hasDefault) {
      this.relationTypeId = relationTypeId;
      this.relationId = relationId;
      this.name = name;
      this.typeId = typeId;
      this.typeModifier = typeModifier;
      this.length = length;
      this.number = number;
      this.nullable = nullable;
      this.autoIncrement = autoIncrement;
      this.numberOfDimensions = numberOfDimensions;
      this.hasDefault = hasDefault;
    }

    public Row() {
    }

    public void load(Context context, ResultBatch resultBatch, int rowIdx) throws IOException  {
      this.relationTypeId = getFieldOfRow(resultBatch, rowIdx, RELATION_TYPE_ID, context, Integer.class);
      this.relationId = getFieldOfRow(resultBatch, rowIdx, RELATION_ID, context, Integer.class);
      this.name = getFieldOfRow(resultBatch, rowIdx, NAME, context, String.class);
      this.typeId = getFieldOfRow(resultBatch, rowIdx, TYPE_ID, context, Integer.class);
      this.typeModifier = getFieldOfRow(resultBatch, rowIdx, TYPE_MOD, context, Integer.class);
      this.length = getFieldOfRow(resultBatch, rowIdx, LENGTH, context, Short.class);
      this.number = getFieldOfRow(resultBatch, rowIdx, NUMBER, context, Short.class);
      this.nullable = getFieldOfRow(resultBatch, rowIdx, NULLABLE, context, Boolean.class);
      this.autoIncrement = getFieldOfRow(resultBatch, rowIdx, AUTOINCREMENT, context, Boolean.class);
      this.numberOfDimensions = getFieldOfRow(resultBatch, rowIdx, NUMBER_OF_DIMS, context, Integer.class);
      this.hasDefault = getFieldOfRow(resultBatch, rowIdx, HAS_DEFAULT, context, Boolean.class);
    }

    public int getRelationTypeId() {
      return relationTypeId;
    }

    public int getRelationId() {
      return relationId;
    }

    public String getName() {
      return name;
    }

    public int getTypeId() {
      return typeId;
    }

    public int getTypeModifier() {
      return typeModifier;
    }

    public short getLength() {
      return length;
    }

    public short getNumber() {
      return number;
    }

    public boolean isNullable() {
      return nullable;
    }

    public boolean isAutoIncrement() {
      return autoIncrement != null ? autoIncrement : false;
    }

    public int getNumberOfDimensions() {
      return numberOfDimensions;
    }

    public boolean isHasDefault() {
      return hasDefault;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      result = prime * result + typeId;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Row other = (Row) obj;
      if (name == null) {
        if (other.name != null)
          return false;
      }
      else if (!name.equals(other.name))
        return false;
      return typeId == other.typeId;
    }

  }

  private static final int RELATION_ID = 0;
  private static final int NAME = 1;
  private static final int TYPE_ID = 2;
  private static final int TYPE_MOD = 3;
  private static final int LENGTH = 4;
  private static final int NUMBER = 5;
  private static final int NULLABLE = 6;
  private static final int AUTOINCREMENT = 7;
  private static final int NUMBER_OF_DIMS = 8;
  private static final int HAS_DEFAULT = 9;
  private static final int RELATION_TYPE_ID = 10;

  public static final PgAttribute INSTANCE = new PgAttribute();

  private PgAttribute() {
  }

  @Override
  public String getSQL(Version currentVersion) {
    return Tables.getSQL(SQL, currentVersion);
  }

  @Override
  public Row createRow(Context context, ResultBatch resultBatch, int rowIdx) throws IOException {
    Row row = new Row();
    row.load(context, resultBatch, rowIdx);
    return row;
  }

  private static final Object[] SQL = {
    Version.get(9, 0, 0),
      " SELECT" +
          " attrelid, attname, atttypid, atttypmod, attlen," +
          " attnum, NOT attnotnull, pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) LIKE '%nextval(%'," +
          " attndims, atthasdef, reltype" +
          " FROM" +
          "   pg_catalog.pg_attribute a" +
          " LEFT JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)" +
          " LEFT JOIN pg_catalog.pg_class c ON (a.attrelid = c.oid)" +
          " WHERE NOT a.attisdropped AND (c.relpersistence <> 't' OR c.relpersistence IS NULL)"
  };

}
