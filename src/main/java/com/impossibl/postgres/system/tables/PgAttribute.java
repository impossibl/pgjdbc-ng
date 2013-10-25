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
 * Table for "pg_attribute"
 *
 * @author kdubb
 *
 */
public class PgAttribute implements Table<PgAttribute.Row> {

  public static class Row {

    public int relationTypeId;
    public int relationId;
    public String name;
    public int typeId;
    public int typeModifier;
    public short length;
    public short number;
    public boolean nullable;
    public boolean autoIncrement;
    public int numberOfDimensions;
    public boolean hasDefault;

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
      if(this == obj)
        return true;
      if(obj == null)
        return false;
      if(getClass() != obj.getClass())
        return false;
      Row other = (Row) obj;
      if(name == null) {
        if(other.name != null)
          return false;
      }
      else if(!name.equals(other.name))
        return false;
      if(typeId != other.typeId)
        return false;
      return true;
    }

  }

  public static final PgAttribute INSTANCE = new PgAttribute();

  private PgAttribute() {
  }

  public String getSQL(Version currentVersion) {
    return Tables.getSQL(SQL, currentVersion);
  }

  public Row createRow() {
    return new Row();
  }

  private static final Object[] SQL = {
      Version.get(9, 0, 0),
      " select " +
      "   attrelid as \"relationId\", attname as \"name\", atttypid as \"typeId\", atttypmod as \"typeModifier\", attlen as \"length\", " +
      "   attnum as \"number\", not attnotnull as \"nullable\", pg_catalog.pg_get_expr(ad.adbin,ad.adrelid) like '%nextval(%' as \"autoIncrement\", " +
      "   attndims as \"numberOfDimensions\", atthasdef as \"hasDefault\", reltype as \"relationTypeId\" " +
      " from " +
      "   pg_catalog.pg_attribute a " +
      " left join pg_catalog.pg_attrdef ad " +
      "   on (a.attrelid = ad.adrelid and a.attnum = ad.adnum)" +
      " left join pg_catalog.pg_class c" +
      "   on (a.attrelid = c.oid)" +
      " where " +
      "   not a.attisdropped"
  };

}
