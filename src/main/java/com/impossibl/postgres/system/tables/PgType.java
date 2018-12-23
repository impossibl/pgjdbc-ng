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
 * Table for "pg_type"
 *
 * @author kdubb
 *
 */
public class PgType implements Table<PgType.Row> {

  public static class Row implements Table.Row {

    private int oid;
    private String name;
    private short length;
    private String discriminator;
    private String category;
    private String deliminator;
    private int relationId;
    private int elementTypeId;
    private int arrayTypeId;
    private String inputId;
    private String outputId;
    private String receiveId;
    private String sendId;
    private String modInId;
    private String modOutId;
    private String alignment;
    private String namespace;
    private int domainBaseTypeId;
    private int domainTypeMod;
    private boolean domainNotNull;
    private String domainDefault;
    private Integer rangeBaseTypeId;

    public Row() {
    }

    public Integer getReferencingTypeOid() {
      if (category.equals("A")) return elementTypeId;
      switch (discriminator) {
        case "d": return domainBaseTypeId;
        case "r": return rangeBaseTypeId;
      }
      return null;
    }

    public boolean isPsuedo() {
      return discriminator.equals("p");
    }

    public boolean isBase() {
      return discriminator.equals("b");
    }

    public boolean isArray() {
      return category.equals("A");
    }

    public void load(Context context, ResultBatch resultBatch, int rowIdx) throws IOException {
      this.oid = getFieldOfRow(resultBatch, rowIdx, OID, context, Integer.class);
      this.name = getFieldOfRow(resultBatch, rowIdx, NAME, context, String.class);
      this.length = getFieldOfRow(resultBatch, rowIdx, LENGTH, context, Short.class);
      this.discriminator = getFieldOfRow(resultBatch, rowIdx, DISCRIMINATOR, context, String.class);
      this.category = getFieldOfRow(resultBatch, rowIdx, CATEGORY, context, String.class);
      this.deliminator = getFieldOfRow(resultBatch, rowIdx, DELIMINATOR, context, String.class);
      this.relationId = getFieldOfRow(resultBatch, rowIdx, RELATION_ID, context, Integer.class);
      this.elementTypeId = getFieldOfRow(resultBatch, rowIdx, ELEMENT_TYPE_ID, context, Integer.class);
      this.arrayTypeId = getFieldOfRow(resultBatch, rowIdx, ARRAY_TYPE_ID, context, Integer.class);
      this.inputId = getFieldOfRow(resultBatch, rowIdx, INPUT_ID, context, String.class);
      this.outputId = getFieldOfRow(resultBatch, rowIdx, OUTPUT_ID, context, String.class);
      this.receiveId = getFieldOfRow(resultBatch, rowIdx, RECEIVE_ID, context, String.class);
      this.sendId = getFieldOfRow(resultBatch, rowIdx, SEND_ID, context, String.class);
      this.modInId = getFieldOfRow(resultBatch, rowIdx, MOD_IN_ID, context, String.class);
      this.modOutId = getFieldOfRow(resultBatch, rowIdx, MOD_OUT_ID, context, String.class);
      this.alignment = getFieldOfRow(resultBatch, rowIdx, ALIGNMENT, context, String.class);
      this.domainBaseTypeId = getFieldOfRow(resultBatch, rowIdx, DOMAIN_BASE_TYPE_ID, context, Integer.class);
      this.domainTypeMod = getFieldOfRow(resultBatch, rowIdx, DOMAIN_TYPE_MOD, context, Integer.class);
      this.domainNotNull = getFieldOfRow(resultBatch, rowIdx, DOMAIN_NOT_NULL, context, Boolean.class);
      this.namespace = getFieldOfRow(resultBatch, rowIdx, NAMESPACE, context, String.class);
      this.domainDefault = getFieldOfRow(resultBatch, rowIdx, DOMAIN_DEFAULT, context, String.class);
      this.rangeBaseTypeId = getFieldOfRow(resultBatch, rowIdx, RANGE_BASE_TYPE_ID, context, Integer.class);
    }

    public int getOid() {
      return oid;
    }

    public void setOid(int v) {
      oid = v;
    }

    public String getName() {
      return name;
    }

    public short getLength() {
      return length;
    }

    public String getDiscriminator() {
      return discriminator;
    }

    public String getCategory() {
      return category;
    }

    public String getDeliminator() {
      return deliminator;
    }

    public int getRelationId() {
      return relationId;
    }

    public int getElementTypeId() {
      return elementTypeId;
    }

    public int getArrayTypeId() {
      return arrayTypeId;
    }

    public String getInputId() {
      return inputId;
    }

    public String getOutputId() {
      return outputId;
    }

    public String getReceiveId() {
      return receiveId;
    }

    public String getSendId() {
      return sendId;
    }

    public String getModInId() {
      return modInId;
    }

    public String getModOutId() {
      return modOutId;
    }

    public String getAlignment() {
      return alignment;
    }

    public int getDomainBaseTypeId() {
      return domainBaseTypeId;
    }

    public int getDomainTypeMod() {
      return domainTypeMod;
    }

    public boolean isDomainNotNull() {
      return domainNotNull;
    }

    public String getNamespace() {
      return namespace;
    }

    public String getDomainDefault() {
      return domainDefault;
    }

    public Integer getRangeBaseTypeId() {
      return rangeBaseTypeId;
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
      return oid == other.oid;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + oid;
      return result;
    }

  }

  private static final int OID = 0;
  private static final int NAME = 1;
  private static final int LENGTH = 2;
  private static final int DISCRIMINATOR = 3;
  private static final int CATEGORY = 4;
  private static final int DELIMINATOR = 5;
  private static final int RELATION_ID = 6;
  private static final int ELEMENT_TYPE_ID = 7;
  private static final int ARRAY_TYPE_ID = 8;
  private static final int INPUT_ID = 9;
  private static final int OUTPUT_ID = 10;
  private static final int RECEIVE_ID = 11;
  private static final int SEND_ID = 12;
  private static final int MOD_IN_ID = 13;
  private static final int MOD_OUT_ID = 14;
  private static final int ALIGNMENT = 15;
  private static final int NAMESPACE = 16;
  private static final int DOMAIN_BASE_TYPE_ID = 17;
  private static final int DOMAIN_TYPE_MOD = 18;
  private static final int DOMAIN_NOT_NULL = 19;
  private static final int DOMAIN_DEFAULT = 20;
  private static final int RANGE_BASE_TYPE_ID = 21;

  public static final PgType INSTANCE = new PgType();

  private PgType() {
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
    Version.get(9, 2, 0),
    " SELECT" +
        " t.oid, typname, typlen, typtype, typcategory, typdelim, typrelid, typelem, typarray," +
        " typinput::text, typoutput::text, typreceive::text, typsend::text, typmodin::text, typmodout::text," +
        " typalign, n.nspname, typbasetype, typtypmod, typnotnull, pg_catalog.pg_get_expr(typdefaultbin,0), rngsubtype" +
        " FROM" +
        "   pg_catalog.pg_type t" +
        " LEFT JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)" +
        " LEFT JOIN pg_catalog.pg_range r ON (t.oid = r.rngtypid)",
    Version.get(9, 1, 0),
    " SELECT" +
        " t.oid, typname, typlen, typtype, typcategory, typdelim, typrelid, typelem, typarray," +
        " typinput::text, typoutput::text, typreceive::text, typsend::text, typmodin::text, typmodout::text," +
        " typalign, n.nspname, typbasetype, typtypmod, typnotnull, pg_catalog.pg_get_expr(typdefaultbin,0), NULL" +
        " FROM" +
        "   pg_catalog.pg_type t" +
        " LEFT JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid)",
  };

}
