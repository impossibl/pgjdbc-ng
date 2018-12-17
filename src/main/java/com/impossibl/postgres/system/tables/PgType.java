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
    private int inputId;
    private int outputId;
    private int receiveId;
    private int sendId;
    private int modInId;
    private int modOutId;
    private String alignment;
    private String namespace;
    private int domainBaseTypeId;
    private int domainTypeMod;
    private boolean domainNotNull;
    private String domainDefault;
    private Integer rangeBaseTypeId;

    public Row() {
    }

    public void load(Context context, ResultBatch.Row rowData) throws IOException {
      this.oid = rowData.getField(OID, context, Integer.class);
      this.name = rowData.getField(NAME, context, String.class);
      this.length = rowData.getField(LENGTH, context, Short.class);
      this.discriminator = rowData.getField(DISCRIMINATOR, context, String.class);
      this.category = rowData.getField(CATEGORY, context, String.class);
      this.deliminator = rowData.getField(DELIMINATOR, context, String.class);
      this.relationId = rowData.getField(RELATION_ID, context, Integer.class);
      this.elementTypeId = rowData.getField(ELEMENT_TYPE_ID, context, Integer.class);
      this.arrayTypeId = rowData.getField(ARRAY_TYPE_ID, context, Integer.class);
      this.inputId = rowData.getField(INPUT_ID, context, Integer.class);
      this.outputId = rowData.getField(OUTPUT_ID, context, Integer.class);
      this.receiveId = rowData.getField(RECEIVE_ID, context, Integer.class);
      this.sendId = rowData.getField(SEND_ID, context, Integer.class);
      this.modInId = rowData.getField(MOD_IN_ID, context, Integer.class);
      this.modOutId = rowData.getField(MOD_OUT_ID, context, Integer.class);
      this.alignment = rowData.getField(ALIGNMENT, context, String.class);
      this.domainBaseTypeId = rowData.getField(DOMAIN_BASE_TYPE_ID, context, Integer.class);
      this.domainTypeMod = rowData.getField(DOMAIN_TYPE_MOD, context, Integer.class);
      this.domainNotNull = rowData.getField(DOMAIN_NOT_NULL, context, Boolean.class);
      this.namespace = rowData.getField(NAMESPACE, context, String.class);
      this.domainDefault = rowData.getField(DOMAIN_DEFAULT, context, String.class);
      this.rangeBaseTypeId = rowData.getField(RANGE_BASE_TYPE_ID, context, Integer.class);
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

    public void setName(String v) {
      name = v;
    }

    public short getLength() {
      return length;
    }

    public void setLength(short v) {
      length = v;
    }

    public String getDiscriminator() {
      return discriminator;
    }

    public void setDiscriminator(String v) {
      discriminator = v;
    }

    public String getCategory() {
      return category;
    }

    public void setCategory(String v) {
      category = v;
    }

    public String getDeliminator() {
      return deliminator;
    }

    public void setDeliminator(String v) {
      deliminator = v;
    }

    public int getRelationId() {
      return relationId;
    }

    public void setRelationId(int v) {
      relationId = v;
    }

    public int getElementTypeId() {
      return elementTypeId;
    }

    public void setElementTypeId(int v) {
      elementTypeId = v;
    }

    public int getArrayTypeId() {
      return arrayTypeId;
    }

    public void setArrayTypeId(int v) {
      arrayTypeId = v;
    }

    public int getInputId() {
      return inputId;
    }

    public void setInputId(int v) {
      inputId = v;
    }

    public int getOutputId() {
      return outputId;
    }

    public void setOutputId(int v) {
      outputId = v;
    }

    public int getReceiveId() {
      return receiveId;
    }

    public void setReceiveId(int v) {
      receiveId = v;
    }

    public int getSendId() {
      return sendId;
    }

    public void setSendId(int v) {
      sendId = v;
    }

    public int getModInId() {
      return modInId;
    }

    public void setModInId(int v) {
      modInId = v;
    }

    public int getModOutId() {
      return modOutId;
    }

    public void setModOutId(int v) {
      modOutId = v;
    }

    public String getAlignment() {
      return alignment;
    }

    public void setAlignment(String v) {
      alignment = v;
    }

    public int getDomainBaseTypeId() {
      return domainBaseTypeId;
    }

    public void setDomainBaseTypeId(int v) {
      domainBaseTypeId = v;
    }

    public int getDomainTypeMod() {
      return domainTypeMod;
    }

    public void setDomainTypeMod(int v) {
      domainTypeMod = v;
    }

    public boolean isDomainNotNull() {
      return domainNotNull;
    }

    public void setDomainNotNull(boolean v) {
      domainNotNull = v;
    }

    public String getNamespace() {
      return namespace;
    }

    public void setNamespace(String v) {
      namespace = v;
    }

    public String getDomainDefault() {
      return domainDefault;
    }

    public void setDomainDefault(String v) {
      domainDefault = v;
    }

    public Integer getRangeBaseTypeId() {
      return rangeBaseTypeId;
    }

    public void setRangeBaseTypeId(Integer v) {
      rangeBaseTypeId = v;
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
      if (oid != other.oid)
        return false;
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + oid;
      return result;
    }

  }

  static final int OID = 0;
  static final int NAME = 1;
  static final int LENGTH = 2;
  static final int DISCRIMINATOR = 3;
  static final int CATEGORY = 4;
  static final int DELIMINATOR = 5;
  static final int RELATION_ID = 6;
  static final int ELEMENT_TYPE_ID = 7;
  static final int ARRAY_TYPE_ID = 8;
  static final int INPUT_ID = 9;
  static final int OUTPUT_ID = 10;
  static final int RECEIVE_ID = 11;
  static final int SEND_ID = 12;
  static final int MOD_IN_ID = 13;
  static final int MOD_OUT_ID = 14;
  static final int ALIGNMENT = 15;
  static final int NAMESPACE = 16;
  static final int DOMAIN_BASE_TYPE_ID = 17;
  static final int DOMAIN_TYPE_MOD = 18;
  static final int DOMAIN_NOT_NULL = 19;
  static final int DOMAIN_DEFAULT = 20;
  static final int RANGE_BASE_TYPE_ID = 21;

  public static final PgType INSTANCE = new PgType();

  private PgType() {
  }

  @Override
  public String getSQL(Version currentVersion) {
    return Tables.getSQL(SQL, currentVersion);
  }

  @Override
  public Row createRow(Context context, ResultBatch.Row source) throws IOException {
    Row row = new Row();
    row.load(context, source);
    return row;
  }

  private static final Object[] SQL = {
    Version.get(9, 2, 0),
    " select" +
      "   t.oid, typname as \"name\", typlen as \"length\", typtype as \"discriminator\", typcategory as \"category\", typdelim as \"deliminator\", typrelid as \"relationId\"," +
      "   typelem as \"elementTypeId\", typarray as \"arrayTypeId\", typinput::oid as \"inputId\", typoutput::oid as \"outputId\", typreceive::oid as \"receiveId\", typsend::oid as \"sendId\"," +
      "   typmodin::oid as \"modInId\", typmodout::oid as \"modOutId\", typalign as alignment, n.nspname as \"namespace\", " +
      "   typbasetype as \"domainBaseTypeId\", typtypmod as \"domainTypeMod\", typnotnull as \"domainNotNull\", pg_catalog.pg_get_expr(typdefaultbin,0) as \"domainDefault\", " +
      "   rngsubtype as \"rangeBaseTypeId\"" +
      " from" +
      "   pg_catalog.pg_type t" +
      " left join pg_catalog.pg_namespace n on (t.typnamespace = n.oid) " +
      " left join pg_catalog.pg_range r on (t.oid = r.rngtypid)",
    Version.get(9, 1, 0),
    " select" +
      "   t.oid, typname as \"name\", typlen as \"length\", typtype as \"discriminator\", typcategory as \"category\", typdelim as \"deliminator\", typrelid as \"relationId\"," +
      "   typelem as \"elementTypeId\", typarray as \"arrayTypeId\", typinput::oid as \"inputId\", typoutput::oid as \"outputId\", typreceive::oid as \"receiveId\", typsend::oid as \"sendId\"," +
      "   typmodin::oid as \"modInId\", typmodout::oid as \"modOutId\", typalign as alignment, n.nspname as \"namespace\", " +
      "   typbasetype as \"domainBaseTypeId\", typtypmod as \"domainTypeMod\", typnotnull as \"domainNotNull\", pg_catalog.pg_get_expr(typdefaultbin,0) as \"domainDefault\" " +
      "   null as \"rangeBaseTypeId\"" +
      " from" +
      "   pg_catalog.pg_type t" +
      " left join pg_catalog.pg_namespace n on (t.typnamespace = n.oid) ",
  };

}
