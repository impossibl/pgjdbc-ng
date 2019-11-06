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

import com.impossibl.postgres.api.data.ACLItem;

import java.text.ParseException;
import java.util.stream.Collectors;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class ACLItemTest {

  @Test
  public void testRightsParsing() throws ParseException {

    ACLItem.Right[] allRightsWithOptions =
        ACLItem.ALL_PRIVILEGES.chars()
            .mapToObj(p -> new ACLItem.Right((char)p, true))
            .toArray(ACLItem.Right[]::new);

    String allRightsWithOptionsStr =
        ACLItem.ALL_PRIVILEGES.chars()
            .mapToObj(p -> (char)p + "*")
            .collect(Collectors.joining());

    assertArrayEquals(allRightsWithOptions, ACLItem.rightsOf(allRightsWithOptionsStr));

    ACLItem.Right[] allRightsWithoutOptions =
        ACLItem.ALL_PRIVILEGES.chars()
            .mapToObj(p -> new ACLItem.Right((char)p, false))
            .toArray(ACLItem.Right[]::new);

    assertArrayEquals(allRightsWithoutOptions, ACLItem.rightsOf(ACLItem.ALL_PRIVILEGES));

    ACLItem.Right[] singleRight = new ACLItem.Right[]{new ACLItem.Right('a', false)};
    assertArrayEquals(singleRight, ACLItem.rightsOf("a"));

    ACLItem.Right[] singleRightWithOption = new ACLItem.Right[]{new ACLItem.Right('a', true)};
    assertArrayEquals(singleRightWithOption, ACLItem.rightsOf("a*"));

    ACLItem.Right[] multipleRightsNoOptions = new ACLItem.Right[]{
        new ACLItem.Right('a', false), new ACLItem.Right('D', false)
    };
    assertArrayEquals(multipleRightsNoOptions, ACLItem.rightsOf("aD"));

    ACLItem.Right[] noRights = new ACLItem.Right[0];
    assertArrayEquals(noRights, ACLItem.rightsOf(""));

    assertArrayEquals(null, ACLItem.rightsOf(null));
  }

  @Test(expected = ParseException.class)
  public void testInvalidRightsParsing1() throws ParseException {
    ACLItem.rightsOf("*");
  }

  @Test(expected = ParseException.class)
  public void testInvalidRightsParsing2() throws ParseException {
    ACLItem.rightsOf("a**");
  }

  @Test(expected = ParseException.class)
  public void testInvalidRightsParsing3() throws ParseException {
    ACLItem.rightsOf("q");
  }

  @Test(expected = ParseException.class)
  public void testInvalidRightsParsing4() throws ParseException {
    ACLItem.rightsOf("*a");
  }

}
