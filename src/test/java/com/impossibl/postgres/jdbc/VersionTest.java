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
package com.impossibl.postgres.jdbc;

import com.impossibl.postgres.system.Version;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;

/**
 * Tests for the Version class
 * @author kdubb
 */
@RunWith(JUnit4.class)
public class VersionTest {

  @Test
  public void testParse() {
    Version ver;

    ver = Version.parse("1");

    assertEquals(ver.getMajor(), 1);
    assertEquals(ver.getMinor(), null);
    assertEquals(ver.getMinorValue(), 0);
    assertEquals(ver.getRevision(), null);
    assertEquals(ver.getRevisionValue(), 0);

    ver = Version.parse("1.2");

    assertEquals(ver.getMajor(), 1);
    assertEquals((int)ver.getMinor(), 2);
    assertEquals(ver.getRevision(), null);
    assertEquals(ver.getRevisionValue(), 0);

    ver = Version.parse("1.2.3.1.2");

    assertEquals(ver.getMajor(), 1);
    assertEquals((int)ver.getMinor(), 2);
    assertEquals((int)ver.getRevision(), 3);

    ver = Version.parse("1.");

    assertEquals(ver.getMajor(), 1);
    assertEquals(ver.getMinor(), null);
    assertEquals(ver.getMinorValue(), 0);
    assertEquals(ver.getRevision(), null);
    assertEquals(ver.getRevisionValue(), 0);

    ver = Version.parse("1.2.");

    assertEquals(ver.getMajor(), 1);
    assertEquals((int)ver.getMinor(), 2);
    assertEquals(ver.getRevision(), null);
    assertEquals(ver.getRevisionValue(), 0);

    ver = Version.parse("9.4devel");

    assertEquals(ver.getMajor(), 9);
    assertEquals((int)ver.getMinor(), 4);
    assertEquals(ver.getRevision(), null);

    try {
      ver = Version.parse("1..3.");
      fail("Version shouldn't be allowed");
    }
    catch (IllegalArgumentException e) {
      // Ok
    }
  }

  @Test
  public void testEqual() {
    Version v921 = Version.parse("9.2.1");

    assertTrue(v921.isEqual(9));
    assertFalse(v921.isEqual(8));
    assertFalse(v921.isEqual(10));

    assertTrue(v921.isEqual(9, 2));
    assertFalse(v921.isEqual(9, 1));
    assertFalse(v921.isEqual(9, 3));

    assertTrue(v921.isEqual(9, 2, 1));
    assertFalse(v921.isEqual(9, 2, 0));
    assertFalse(v921.isEqual(9, 2, 2));

    Version v930 = Version.parse("9.3");

    assertFalse(v921.isEqual(v930));
    assertFalse(v930.isEqual(v921));
  }

  @Test
  public void testMinimumVersion() {
    Version v8422 = Version.parse("8.4.22");
    Version v843 = Version.parse("8.4.3");
    Version v921 = Version.parse("9.2.1");
    Version v930 = Version.parse("9.3");

    assertTrue(v8422.isMinimum(v843));
    assertFalse(v843.isMinimum(v8422));

    assertTrue(v921.isMinimum(v8422));
    assertFalse(v921.isMinimum(v930));

    assertTrue(v921.isMinimum(v921));
  }
}
