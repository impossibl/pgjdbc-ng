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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/**
 * Created by dstipp on 12/7/15.
 */
@RunWith(JUnit4.class)
public class TablesTest {

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetSQL() {

    assertEquals(Tables.getSQL(SQL, Version.parse("9.0.0")), SQL[3]);
    assertNotEquals(Tables.getSQL(SQL, Version.parse("9.0.0")), SQL[1]);

    assertEquals(Tables.getSQL(SQL, Version.parse("9.4.5")), SQL[1]);
  }

  @Test
  public void testIllegalStateException() {
    try {
      String sql = Tables.getSQL(SQLBad, Version.parse("9.0.0"));
      fail("Didn't hit IllegalStateException, usually from bad sqlData");
    }
    catch (IllegalStateException e) {
      // Ok
    }
  }

  @Test
  public void testUnsupportedServerVersion() {
    thrown.expect(UnsupportedServerVersion.class);
    assertEquals(Tables.getSQL(SQL, Version.parse("8.3.0")), SQL[1]);
  }


  private static final Object[] SQL = {
    Version.get(9, 4, 2),
    "select '9.4.2'",
    Version.get(9, 0, 0),
    "select '9.0.0'",
  };

  private static final Object[] SQLBad = {
      Version.get(9, 0, 0),
  };

}
