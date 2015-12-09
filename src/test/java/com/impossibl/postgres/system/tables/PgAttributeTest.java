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

import com.impossibl.postgres.system.UnsupportedServerVersion;
import com.impossibl.postgres.system.Version;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by dstipp on 12/8/15.
 */
@RunWith(JUnit4.class)
public class PgAttributeTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetSQLVersionEqual() throws Exception {
    assertEquals(PgAttribute.INSTANCE.getSQL(Version.parse("9.0.0")), SQL[1]);
  }

  @Test
  public void testGetSQLVersionGreater() throws Exception {
    assertEquals(PgAttribute.INSTANCE.getSQL(Version.parse("9.4.5")), SQL[1]);
  }

  @Test
  public void testGetSQLVersionInvalid() throws Exception {
    thrown.expect(UnsupportedServerVersion.class);
    assertEquals(PgAttribute.INSTANCE.getSQL(Version.parse("8.0.0")), SQL[1]);
  }

  @Test
  public void testRow() throws Exception {
    assertEquals(new PgAttribute.Row(), PgAttribute.INSTANCE.createRow());
  }

  @Test
  public void testHashCode() throws Exception {
    PgAttribute.Row pgAttrOne = createRow(81, 1255, "proname", 19, -1, (short) 64, (short) 1, false, false, 0, false);
    PgAttribute.Row pgAttrOneAgain = createRow(81, 1255, "proname", 19, -1, (short) 64, (short) 1, false, false, 0, false);
    PgAttribute.Row pgAttrTwo = createRow(81, 1255, "pronamespace", 26, -1, (short) 4, (short) 2, false, false, 0, false);

    assertEquals(pgAttrOne.hashCode(), pgAttrOne.hashCode());
    assertEquals(pgAttrOne.hashCode(), pgAttrOneAgain.hashCode());
    assertNotEquals(pgAttrOne.hashCode(), pgAttrTwo.hashCode());
  }

  @Test
  public void testEquals() throws Exception {
    PgAttribute.Row pgAttrOne = createRow(81, 1255, "proname", 19, -1, (short) 64, (short) 1, false, false, 0, false);
    PgAttribute.Row pgAttrOneAgain = createRow(81, 1255, "proname", 19, -1, (short) 64, (short) 1, false, false, 0, false);
    PgAttribute.Row pgAttrOneNullName = createRow(81, 1255, null, 19, -1, (short) 64, (short) 1, false, false, 0, false);
    PgAttribute.Row pgAttrOneAlternateTypeId = createRow(81, 1255, "proname", 0, -1, (short) 64, (short) 1, false, false, 0, false);
    PgAttribute.Row pgAttrTwo = createRow(81, 1255, "pronamespace", 26, -1, (short) 4, (short) 2, false, false, 0, false);

    assertTrue(pgAttrOne.equals(pgAttrOne));
    assertFalse(pgAttrOne.equals(null));
    assertFalse(pgAttrOne.equals("testStringNotSameClass"));
    assertFalse(pgAttrOne.equals(pgAttrTwo));
    assertFalse(pgAttrOneNullName.equals(pgAttrOne));
    assertFalse(pgAttrOne.equals(pgAttrOneAlternateTypeId));
    assertTrue(pgAttrOne.equals(pgAttrOneAgain));

  }

  /*
    -[ RECORD 1 ]------+------------------------------------
  relationId         | 1255
  name               | proname
  typeId             | 19
  typeModifier       | -1
  length             | 64
  number             | 1
  nullable           | f
  autoIncrement      | [NULL]
  numberOfDimensions | 0
  hasDefault         | f
  relationTypeId     | 81
  -[ RECORD 2 ]------+------------------------------------
  relationId         | 1255
  name               | pronamespace
  typeId             | 26
  typeModifier       | -1
  length             | 4
  number             | 2
  nullable           | f
  autoIncrement      | [NULL]
  numberOfDimensions | 0
  hasDefault         | f
  relationTypeId     | 81
  */
  private PgAttribute.Row createRow(int relationTypeId,
                                    int relationId,
                                    String name,
                                    int typeId,
                                    int typeModifier,
                                    short length,
                                    short number,
                                    boolean nullable,
                                    boolean autoIncrement,
                                    int numberOfDimensions,
                                    boolean hasDefault) {
    PgAttribute.Row pgAttrRow = new PgAttribute.Row();

    pgAttrRow.relationTypeId = relationTypeId;
    pgAttrRow.relationId = relationId;
    pgAttrRow.name = name;
    pgAttrRow.typeId = typeId;
    pgAttrRow.typeModifier = typeModifier;
    pgAttrRow.length = length;
    pgAttrRow.number = number;
    pgAttrRow.nullable = nullable;
    pgAttrRow.autoIncrement = autoIncrement;
    pgAttrRow.numberOfDimensions = numberOfDimensions;
    pgAttrRow.hasDefault = hasDefault;

    return pgAttrRow;
  }

  // copy-paste from PgAttribute.  Has to be a better way than this, but IDE and dp4j don't seem to get along.
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
