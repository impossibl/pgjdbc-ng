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

import com.impossibl.postgres.system.Version;


/**
 * Table for "pg_type"
 *
 * @author kdubb
 *
 */
public class PgType implements Table<PgType.Row> {

  public static class Row {

    public int oid;
    public String name;
    public short length;
    public String discriminator;
    public String category;
    public String deliminator;
    public int relationId;
    public int elementTypeId;
    public int arrayTypeId;
    public int inputId;
    public int outputId;
    public int receiveId;
    public int sendId;
    public int modInId;
    public int modOutId;
    public int analyzeId;
    public String alignment;
    public int domainBaseTypeId;
    public int domainTypeMod;
    public boolean domainNotNull;
    public int domainDimensions;
    public String namespace;
    public String domainDefault;
    public int rangeBaseTypeId;

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

  public static final PgType INSTANCE = new PgType();

  private PgType() {
  }

  @Override
  public String getSQL(Version currentVersion) {
    return Tables.getSQL(SQL, currentVersion);
  }

  @Override
  public Row createRow() {
    return new Row();
  }

  private static final Object[] SQL = {
    Version.get(9, 2, 0),
    " select" +
      "   t.oid, typname as \"name\", typlen as \"length\", typtype as \"discriminator\", typcategory as \"category\", typdelim as \"deliminator\", typrelid as \"relationId\"," +
      "   typelem as \"elementTypeId\", typarray as \"arrayTypeId\", typinput::oid as \"inputId\", typoutput::oid as \"outputId\", typreceive::oid as \"receiveId\", typsend::oid as \"sendId\"," +
      "   typmodin::oid as \"modInId\", typmodout::oid as \"modOutId\", typalign as alignment, n.nspname as \"namespace\", " +
      "   typbasetype as \"domainBaseTypeId\", typtypmod as \"domainTypeMod\", typnotnull as \"domainNotNull\", pg_catalog.pg_get_expr(typdefaultbin,0) as \"domainDefault\", " +
      "   rngsubtype as \"rangeBaseTypeId\" " +
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
      " from" +
      "   pg_catalog.pg_type t" +
      " left join pg_catalog.pg_namespace n on (t.typnamespace = n.oid) ",
  };

}
