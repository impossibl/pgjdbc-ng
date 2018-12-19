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
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for PGXADataSource
 * @author <a href="mailto:jesper.pedersen@redhat.com">Jesper Pedersen</a>
 */
@RunWith(JUnit4.class)
public class XADataSourceTest {

  private XAConnection con;
  private XAConnection con2;

  @Before
  public void before() throws Exception {
    PGXADataSource ds = new PGXADataSource();
    ds.setHost(TestUtil.getServer());
    ds.setPort(Integer.valueOf(TestUtil.getPort()));
    ds.setDatabase(TestUtil.getDatabase());
    ds.setUser(TestUtil.getUser());
    ds.setPassword(TestUtil.getPassword());

    con = ds.getXAConnection();
    con2 = ds.getXAConnection();
  }

  @After
  public void after() throws Exception {
    TestUtil.closeDB(con);
    TestUtil.closeDB(con2);
  }

  /*
   * Test getXAConnection()
   */
  @Test
  public void testGetXAConnection() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    Connection c = con.getConnection();
    assertNotNull(c);
    assertTrue(c instanceof PGConnection);
    assertTrue(c.isValid(5));
  }

  /*
   * Test XAResource::start()
   */
  @Test
  public void testXAResourceStart() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // TMENDRSCAN
    try {
      xaRes.start(new XidImpl(0), XAResource.TMENDRSCAN);
      fail("TMENDRSCAN");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMFAIL
    try {
      xaRes.start(new XidImpl(0), XAResource.TMFAIL);
      fail("TMFAIL");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMONEPHASE
    try {
      xaRes.start(new XidImpl(0), XAResource.TMONEPHASE);
      fail("TMONEPHASE");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMSTARTRSCAN
    try {
      xaRes.start(new XidImpl(0), XAResource.TMSTARTRSCAN);
      fail("TMSTARTRSCAN");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMSUCCESS
    try {
      xaRes.start(new XidImpl(0), XAResource.TMSUCCESS);
      fail("TMSUCCESS");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMSUSPEND
    try {
      xaRes.start(new XidImpl(0), XAResource.TMSUSPEND);
      fail("TMSUSPEND");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // Null Xid
    try {
      xaRes.start(null, XAResource.TMNOFLAGS);
      fail("Xid is null");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMRESUME
    try {
      xaRes.start(new XidImpl(0), XAResource.TMRESUME);
      fail("TMRESUME not supported");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    // TMJOIN
    try {
      xaRes.start(new XidImpl(0), XAResource.TMJOIN);
      fail("TMJOIN not supported as initial flag");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }


    XidImpl x0 = new XidImpl(0);
    Connection c = con.getConnection();

    c.setAutoCommit(true);

    xaRes.start(x0, XAResource.TMNOFLAGS);

    try {
      xaRes.start(new XidImpl(1), XAResource.TMJOIN);
      fail("XAResource already active");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_PROTO, e.errorCode);
    }

    try {
      xaRes.end(x0, XAResource.TMSUCCESS);
      xaRes.start(new XidImpl(1), XAResource.TMJOIN);
      fail("TMJOIN only supported with existing Xid");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
      assertFalse(c.getAutoCommit());
      xaRes.start(x0, XAResource.TMJOIN);
    }

    xaRes.end(x0, XAResource.TMSUCCESS);
    xaRes.commit(x0, true);

    assertTrue(c.getAutoCommit());
  }

  /*
   * Test XAResource::end()
   */
  @Test
  public void testXAResourceEnd() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // TMENDRSCAN
    try {
      xaRes.end(new XidImpl(0), XAResource.TMENDRSCAN);
      fail("TMENDRSCAN");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMJOIN
    try {
      xaRes.end(new XidImpl(0), XAResource.TMJOIN);
      fail("TMJOIN");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMNOFLAGS
    try {
      xaRes.end(new XidImpl(0), XAResource.TMNOFLAGS);
      fail("TMNOFLAGS");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMONEPHASE
    try {
      xaRes.end(new XidImpl(0), XAResource.TMONEPHASE);
      fail("TMONEPHASE");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMRESUME
    try {
      xaRes.end(new XidImpl(0), XAResource.TMRESUME);
      fail("TMRESUME");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMSTARTRSCAN
    try {
      xaRes.start(new XidImpl(0), XAResource.TMSTARTRSCAN);
      fail("TMSTARTRSCAN");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // Null Xid
    try {
      xaRes.end(null, XAResource.TMSUCCESS);
      fail("Xid is null");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    try {
      xaRes.end(new XidImpl(0), XAResource.TMSUCCESS);
      fail("XAResource not active");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_PROTO, e.errorCode);
    }

    XidImpl x0 = new XidImpl(0);

    xaRes.start(x0, XAResource.TMNOFLAGS);

    try {
      xaRes.end(new XidImpl(1), XAResource.TMSUCCESS);
      fail("Wrong Xid instance");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_PROTO, e.errorCode);
    }

    try {
      xaRes.end(x0, XAResource.TMSUSPEND);
      fail("TMSUSPEND not supported");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    xaRes.end(x0, XAResource.TMSUCCESS);
    xaRes.commit(x0, true);
  }

  /*
   * Test XAResource::prepare()
   */
  @Test
  public void testXAResourcePrepare() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // Null Xid
    try {
      xaRes.prepare(null);
      fail("Xid is null");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    XidImpl x0 = new XidImpl(0);

    xaRes.start(x0, XAResource.TMNOFLAGS);

    try {
      xaRes.prepare(new XidImpl(1));
      fail("Wrong Xid instance");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    try {
      xaRes.prepare(x0);
      fail("Prepare before end");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    xaRes.end(x0, XAResource.TMSUCCESS);
    xaRes.prepare(x0);
    xaRes.commit(x0, false);
  }

  /*
   * Test XAResource::commit(xyz, true)
   */
  @Test
  public void testXAResourceCommitOnePhase() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // Null Xid
    try {
      xaRes.commit(null, true);
      fail("Xid is null");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // Not associated
    try {
      xaRes.commit(new XidImpl(0), true);
      fail("No Xid associated");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    XidImpl x0 = new XidImpl(0);

    xaRes.start(x0, XAResource.TMNOFLAGS);

    try {
      xaRes.commit(x0, true);
      fail("XAResource is not in an end state");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_PROTO, e.errorCode);
    }

    xaRes.end(x0, XAResource.TMSUCCESS);
    xaRes.commit(x0, true);
  }

  /*
   * Test XAResource::commit(xyz, false)
   */
  @Test
  public void testXAResourceCommitTwoPhase() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // Null Xid
    try {
      xaRes.commit(null, false);
      fail("Xid is null");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // Not associated
    try {
      xaRes.commit(new XidImpl(0), false);
      fail("No Xid associated");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_NOTA, e.errorCode);
    }

    XidImpl x0 = new XidImpl(0);

    xaRes.start(x0, XAResource.TMNOFLAGS);

    try {
      xaRes.commit(x0, false);
      fail("XAResource is not in an end state");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    xaRes.end(x0, XAResource.TMSUCCESS);

    try {
      xaRes.commit(x0, false);
      fail("XAResource is not prepared");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_RMERR, e.errorCode);
    }

    xaRes.prepare(x0);
    xaRes.commit(x0, false);
  }

  /*
   * Test XAResource::rollback()
   */
  @Test
  public void testXAResourceRollback() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // Null Xid
    try {
      xaRes.rollback(null);
      fail("Xid is null");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // Unknown Xid
    try {
      xaRes.rollback(new XidImpl(0));
      fail("Unknown Xid");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_NOTA, e.errorCode);
    }

    XidImpl x0 = new XidImpl(0);

    // "One phase"
    xaRes.start(x0, XAResource.TMNOFLAGS);
    xaRes.rollback(x0);

    // "Two phase"
    xaRes.start(x0, XAResource.TMNOFLAGS);
    xaRes.end(x0, XAResource.TMFAIL);
    xaRes.prepare(x0);
    xaRes.rollback(x0);
  }

  /*
   * Test XAResource::recover()
   */
  @Test
  public void testXAResourceRecover() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    // TMFAIL
    try {
      xaRes.recover(XAResource.TMFAIL);
      fail("TMFAIL");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMJOIN
    try {
      xaRes.recover(XAResource.TMJOIN);
      fail("TMJOIN");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMONEPHASE
    try {
      xaRes.recover(XAResource.TMONEPHASE);
      fail("TMONEPHASE");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMRESUME
    try {
      xaRes.recover(XAResource.TMRESUME);
      fail("TMRESUME");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMSUCCESS
    try {
      xaRes.recover(XAResource.TMSUCCESS);
      fail("TMSUCCESS");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    // TMSUSPEND
    try {
      xaRes.recover(XAResource.TMSUSPEND);
      fail("TMSUSPEND");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_INVAL, e.errorCode);
    }

    Xid[] xids = xaRes.recover(XAResource.TMSTARTRSCAN);
    assertNotNull(xids);
    assertEquals(0, xids.length);

    xids = xaRes.recover(XAResource.TMENDRSCAN);
    assertNotNull(xids);
    assertEquals(0, xids.length);
  }

  /*
   * Test XAResource::forget()
   */
  @Test
  public void testXAResourceForget() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    try {
      xaRes.forget(new XidImpl(0));
      fail("Forget not supported");
    }
    catch (XAException e) {
      assertEquals(XAException.XAER_NOTA, e.errorCode);
    }
  }

  /*
   * Test XAResource transaction timeout
   */
  @Test
  public void testXAResourceTransactionTimeout() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();

    assertFalse(xaRes.setTransactionTimeout(5));
    assertEquals(0, xaRes.getTransactionTimeout());
  }

  /*
   * Test XAResource::isSameRM
   */
  @Test
  public void testXAResourceIsSameRM() throws Exception {
    assertNotNull(con);
    assertFalse(con instanceof PGConnection);
    assertTrue(con instanceof PGXAConnection);
    assertTrue(con instanceof XAResource);

    assertNotNull(con2);
    assertFalse(con2 instanceof PGConnection);
    assertTrue(con2 instanceof PGXAConnection);
    assertTrue(con2 instanceof XAResource);

    assumeTrue(hasXA());

    XAResource xaRes = con.getXAResource();
    XAResource xaRes2 = con2.getXAResource();

    assertTrue(xaRes.isSameRM(xaRes));
    assertFalse(xaRes.isSameRM(xaRes2));
  }

  /**
   * Has XA
   * @return True if max_prepared_transactions > 0, otherwise false
   */
  private boolean hasXA() {
    XAResource xaRes = null;
    XidImpl x0 = new XidImpl(0);

    try {
      xaRes = con.getXAResource();
      xaRes.start(x0, XAResource.TMNOFLAGS);
      xaRes.end(x0, XAResource.TMSUCCESS);
      xaRes.prepare(x0);
      xaRes.commit(x0, false);
      return true;
    }
    catch (Exception e) {
      if (xaRes != null) {
        try {
          xaRes.rollback(x0);
        }
        catch (XAException rbe) {
          // Ignore
        }
      }
    }
    return false;
  }

  /**
   * Basic Xid implementation
   */
  static class XidImpl implements Xid {
    private int id;

    /**
     * Constructor
     * @param id The identifier
     */
    XidImpl(int id) {
      this.id = id;
    }

    /**
     * {@inheritDoc}
     */
    public int getFormatId() {
      return id;
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getGlobalTransactionId() {
      return new byte[] {Integer.valueOf(id).byteValue()};
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getBranchQualifier() {
      return new byte[] {Integer.valueOf(id).byteValue()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
      if (other == this)
        return true;

      if (other == null || !(other instanceof XidImpl))
        return false;

      XidImpl x = (XidImpl)other;

      return id == x.id;
    }
  }
}
