package com.impossibl.postgres.jdbc;

import junit.framework.TestCase;

import com.impossibl.postgres.system.Version;

/**
 * Tests for the Version class
 * 
 * @author kdubb
 *
 */
public class VersionTest extends TestCase {
	
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
		
		try {
		  ver = Version.parse("1..3.");
		  fail("Version shouldn't be allowed");
		}
		catch(IllegalArgumentException e) {
		}
		
	}
	
	public void testEqual() {
	  
	  Version v921 = Version.parse("9.2.1");

	  assertTrue(v921.isEqual(9));
    assertFalse(v921.isEqual(8));
    assertFalse(v921.isEqual(10));
    
    assertTrue(v921.isEqual(9,2));
    assertFalse(v921.isEqual(9,1));
    assertFalse(v921.isEqual(9,3));
    
    assertTrue(v921.isEqual(9,2,1));
    assertFalse(v921.isEqual(9,2,0));
    assertFalse(v921.isEqual(9,2,2));
    
    Version v930 = Version.parse("9.3");
    
    assertFalse(v921.isEqual(v930));
    assertFalse(v930.isEqual(v921));
    
	}

}
