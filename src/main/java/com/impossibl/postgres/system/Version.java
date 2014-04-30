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
package com.impossibl.postgres.system;

import java.util.HashMap;
import java.util.regex.Pattern;

public class Version {

  private static final Pattern VERSION_SPLIT_PATTERN = Pattern.compile("\\.");
  private static final HashMap<Version, Version> all = new HashMap<Version, Version>();

  private int major;
  private Integer minor;
  private Integer revision;

  public static Version parse(String versionString) {
    String[] version = VERSION_SPLIT_PATTERN.split(versionString);

    int major = parsePart(version, 0);
    Integer minor = parsePart(version, 1);
    Integer revision = parsePart(version, 2);

    return get(major, minor, revision);
  }

  private static Integer parsePart(String[] version, int idx) {
    if (version.length <= idx)
      return null;

    String part = version[idx];
    if (part == null || part.isEmpty())
      return null;

    return Integer.valueOf(part);
  }

  public static synchronized Version get(int major, Integer minor, Integer revision) {

    Version test = new Version(major, minor, revision);

    Version found = all.get(test);
    if (found == null) {

      all.put(test, test);
      found = test;
    }

    return found;
  }

  private Version(int major, Integer minor, Integer revision) {
    if (minor == null && revision != null) {
      throw new IllegalArgumentException("revision cannot have value when minor does not");
    }

    this.major = major;
    this.minor = minor;
    this.revision = revision;
  }

  public int getMajor() {
    return major;
  }

  public Integer getMinor() {
    return minor;
  }

  public int getMinorValue() {
    return minor != null ? minor : 0;
  }

  public Integer getRevision() {
    return revision;
  }

  public int getRevisionValue() {
    return revision != null ? revision : 0;
  }

  public boolean isMinimum(int major) {
    return this.major >= major;
  }

  public boolean isMinimum(int major, int minor) {
    if (this.major < major)
      return false;
    if (this.major > major)
      return true;
    return getMinorValue() >= minor;
  }

  public boolean isMinimum(int major, int minor, int revision) {
    if (this.major < major)
      return false;
    if (this.major > major)
      return true;
    if (this.minor < minor)
      return false;
    if (this.minor > minor)
      return true;
    return getRevisionValue() >= revision;
  }

  public boolean isMinimum(Version ver) {
    return isMinimum(ver.getMajor(), ver.getMinorValue(), ver.getRevisionValue());
  }

  public boolean isEqual(int major) {
    return major == this.major;
  }

  public boolean isEqual(int major, int minor) {
    return major == this.major && getMinorValue() == minor;
  }

  public boolean isEqual(int major, int minor, int revision) {
    return major == this.major && getMinorValue() == minor && getRevisionValue() == revision;
  }

  public boolean isEqual(Version ver) {
    return isEqual(ver.getMajor(), ver.getMinorValue(), ver.getRevisionValue());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(major);
    if (minor != null) {
      sb.append('.').append(minor);
      if (revision != null) {
        sb.append('.').append(revision);
      }
    }
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + major;
    result = prime * result + ((minor == null) ? 0 : minor.hashCode());
    result = prime * result + ((revision == null) ? 0 : revision.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Version other = (Version) obj;
    if (major != other.major)
      return false;
    if (minor == null) {
      if (other.minor != null)
        return false;
    }
    else if (!minor.equals(other.minor))
      return false;
    if (revision == null) {
      if (other.revision != null)
        return false;
    }
    else if (!revision.equals(other.revision))
      return false;
    return true;
  }

}
