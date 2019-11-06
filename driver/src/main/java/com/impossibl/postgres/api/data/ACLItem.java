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
package com.impossibl.postgres.api.data;

import com.impossibl.postgres.utils.guava.Preconditions;

import static com.impossibl.postgres.utils.guava.Strings.isNullOrEmpty;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ACLItem {

  public static final String ALL_PRIVILEGES = "arwdDxtXUCTc";
  public static final char[] ALL_PRIVILEGE_CHARS = ALL_PRIVILEGES.toCharArray();
  public static final Right[] ALL_RIGHTS = _rightsOf(ALL_PRIVILEGES);

  public static class Right {

    private char privilege;
    private boolean grantOption;

    public Right(char privilege, boolean grantOption) {
      Preconditions.checkArgument(isPrivilege(privilege));
      this.privilege = privilege;
      this.grantOption = grantOption;
    }

    public char getPrivilege() {
      return privilege;
    }

    public boolean isGrantOption() {
      return grantOption;
    }

    @Override
    public String toString() {
      return privilege + (grantOption ? "*" : "");
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Right right = (Right) o;
      return privilege == right.privilege &&
          grantOption == right.grantOption;
    }

    @Override
    public int hashCode() {
      return Objects.hash(privilege, grantOption);
    }
  }

  private String user;
  private Right[] rights;
  private String grantor;

  /**
   * Test is a character is a privilege code.
   */
  public static boolean isPrivilege(char ch) {
    for (int c = 0; c < ALL_PRIVILEGE_CHARS.length; ++c) {
      if (ALL_PRIVILEGE_CHARS[c] == ch) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parse a rights string into rights objects.
   * Because privilege strings are also valid rights string the parsing
   * supports both formats.
   *
   * @param rightsOrPrivileges String of rights or privileges to parse.
   * @return Array of {@link Right}s representing the string of rights or privileges.
   * @throws ParseException If a privilege is not an alphabetic character.
   */
  public static Right[] rightsOf(String rightsOrPrivileges) throws ParseException {
    if (rightsOrPrivileges == null) {
      return null;
    }
    List<Right> rights = new ArrayList<>();
    for (int c = 0; c < rightsOrPrivileges.length(); ++c) {
      char privilege = rightsOrPrivileges.charAt(c);
      if (!isPrivilege(privilege)) {
        throw new ParseException("Invalid rights strings, unrecognized privilege", c);
      }
      boolean grantOption = false;
      int nc = c + 1;
      if (nc < rightsOrPrivileges.length() && rightsOrPrivileges.charAt(nc) == '*') {
        c += 1;
        grantOption = true;
      }
      rights.add(new Right(privilege, grantOption));
    }
    return rights.toArray(new Right[0]);
  }

  private static Right[] _rightsOf(String rightsOrPrivileges) {
    try {
      return rightsOf(rightsOrPrivileges);
    }
    catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Produce a string of privileges matching the provided rights.
   * Grant options from the each right are ignored.
   *
   * @param rights Rights to serialize
   * @return Privilege string matching the provided rights
   */
  public static char[] privilegesOf(Right[] rights) {
    if (rights == null) {
      return null;
    }
    char[] privileges = new char[rights.length];
    for (int c = 0; c < rights.length; ++c) {
      privileges[c] = rights[c].privilege;
    }
    return privileges;
  }

  public ACLItem(String user, String rightsOrPrivileges, String grantor) {
    this(user, _rightsOf(rightsOrPrivileges), grantor);
  }

  public ACLItem(String user, Right[] rights, String grantor) {
    super();
    this.user = user;
    this.rights = rights;
    this.grantor = grantor;
  }

  private ACLItem() {
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPrivileges() {
    char[] privileges = privilegesOf(rights);
    if (privileges == null) {
      return null;
    }
    return new String(privileges);
  }

  public void setPrivileges(String privileges) {
    try {
      setRights(privileges);
    }
    catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public Right[] getRights() {
    return rights;
  }

  public void setRights(Right[] rights) {
    this.rights = rights;
  }

  public void setRights(String rights) throws ParseException {
    this.rights = rightsOf(rights);
  }

  public String getGrantor() {
    return grantor;
  }

  public void setGrantor(String grantor) {
    this.grantor = grantor;
  }

  @Override
  public String toString() {

    StringBuilder sb = new StringBuilder();

    if (user != null && !user.equals("PUBLIC")) {
      sb.append(user);
    }

    sb.append('=');

    if (rights != null) {
      for (Right right : rights) {
        sb.append(right);
      }
    }

    sb.append('/');

    if (grantor != null) {
      sb.append(grantor);
    }

    return sb.toString();
  }

  private static final Pattern ACL_PATTERN = Pattern.compile("(.*)=((?:\\w\\*?)*)/(.*)");

  public static ACLItem parse(String aclItemStr) throws ParseException {

    ACLItem aclItem = null;

    Matcher aclMatcher = ACL_PATTERN.matcher(aclItemStr);
    if (aclMatcher.matches()) {

      aclItem = new ACLItem();

      aclItem.user = aclMatcher.group(1);
      if (isNullOrEmpty(aclItem.user)) {
        aclItem.user = "PUBLIC";
      }

      aclItem.rights = rightsOf(aclMatcher.group(2));
      aclItem.grantor = aclMatcher.group(3);

    }

    return aclItem;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ACLItem aclItem = (ACLItem) o;
    return Objects.equals(user, aclItem.user) &&
        Arrays.equals(rights, aclItem.rights) &&
        Objects.equals(grantor, aclItem.grantor);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(user, grantor);
    result = 31 * result + Arrays.hashCode(rights);
    return result;
  }

}
