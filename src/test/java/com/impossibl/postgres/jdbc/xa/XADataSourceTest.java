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
/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2004-2011, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
package com.impossibl.postgres.jdbc.xa;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.jdbc.TestUtil;

import java.sql.Connection;

import javax.sql.XAConnection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for PGXADataSource
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
@RunWith(JUnit4.class)
public class XADataSourceTest {

  private XAConnection con;

  @Before
  public void before() throws Exception {
    PGXADataSource ds = new PGXADataSource();
    ds.setHost(TestUtil.getServer());
    ds.setPort(Integer.valueOf(TestUtil.getPort()));
    ds.setDatabase(TestUtil.getDatabase());
    ds.setUser(TestUtil.getUser());
    ds.setPassword(TestUtil.getPassword());

    con = ds.getXAConnection();
  }

  @After
  public void after() throws Exception {
    TestUtil.closeDB(con);
  }

  /*
   * Test getXAConnection()
   */
  @Test
  public void testGetXAConnection() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);

    Connection c = con.getConnection();
    assertNotNull(c);
    assertTrue(c instanceof PGConnection);
    assertTrue(c.isValid(5));
  }
}
