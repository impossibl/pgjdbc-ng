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

import static com.impossibl.postgres.system.tables.PGTypeTable.INSTANCE;
import static com.impossibl.postgres.system.tables.PGTypeTable.SQL;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by dstipp on 12/8/15.
 */
public class PGTypeTableTest {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetSQLVersionEqual() {
    assertEquals(INSTANCE.getSQL(Version.parse("9.2.0")), SQL[1]);
    assertNotEquals(INSTANCE.getSQL(Version.parse("9.1.0")), INSTANCE.getSQL(Version.parse("9.2.0")));
  }

  @Test
  public void testGetSQLVersionGreater() {
    assertEquals(INSTANCE.getSQL(Version.parse("9.4.5")), SQL[1]);
  }

  @Test
  public void testGetSQLVersionLess() {
    assertEquals(INSTANCE.getSQL(Version.parse("9.1.9")), SQL[3]);
  }

  @Test
  public void testGetSQLVersionInvalid() {
    thrown.expect(UnsupportedServerVersion.class);
    assertEquals(INSTANCE.getSQL(Version.parse("8.0.0")), SQL[1]);
  }

  @Test
  public void testHashCode() {
    PGTypeTable.Row pgAttrOne = createRow(12345);
    PGTypeTable.Row pgAttrOneAgain = createRow(12345);
    PGTypeTable.Row pgAttrTwo = createRow(54321);

    assertEquals(pgAttrOne.hashCode(), pgAttrOne.hashCode());
    assertEquals(pgAttrOne.hashCode(), pgAttrOneAgain.hashCode());
    assertNotEquals(pgAttrOne.hashCode(), pgAttrTwo.hashCode());
  }

  @Test
  public void testEquals() {
    PGTypeTable.Row pgAttrOne = createRow(12345);
    PGTypeTable.Row pgAttrOneAgain = createRow(12345);
    PGTypeTable.Row pgAttrTwo = createRow(54321);

    assertEquals(pgAttrOne, pgAttrOne);
    assertNotEquals(null, pgAttrOne);
    assertNotEquals("testStringNotSameClass", pgAttrOne);
    assertNotEquals(pgAttrOne, pgAttrTwo);
    assertEquals(pgAttrOne, pgAttrOneAgain);

  }

  private PGTypeTable.Row createRow(int oid) {
    PGTypeTable.Row pgTypeRow = new PGTypeTable.Row();
    pgTypeRow.setOid(oid);
    return pgTypeRow;
  }

}
