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

import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.sql.SQLException;
import java.sql.Savepoint;


/**
 * Reference to a savepoint
 *
 * @author kdubb
 *
 */
class PGSavepoint implements Savepoint {

  private Integer id;
  private String name;
  private boolean released;

  PGSavepoint(int id) {
    this.id = id;
  }

  PGSavepoint(String name) {
    this.name = name;
  }

  /**
   * Ensure the savepoint is valid
   *
   * @throws SQLException
   *          If the connection is invalid
   */
  void checkValid() throws SQLException {

    if (!isValid())
      throw new SQLException("Invalid savepoint");
  }

  @Override
  public int getSavepointId() throws SQLException {
    checkValid();

    if (id == null)
      throw new SQLException("named savepoints have no id");
    return id;
  }

  @Override
  public String getSavepointName() throws SQLException {
    checkValid();

    if (name == null)
      throw new SQLException("auto savepoints have no name");
    return name;
  }

  public String getId() {
    if (id != null)
      return "sp_" + id.toString();
    if (name != null)
      return Identifiers.escape(name);
    throw new IllegalStateException();
  }

  public boolean isValid() {
    return id != null || name != null;
  }

  public void invalidate() {
    id = null;
    name = null;
  }

  public boolean getReleased() {
    return released;
  }

  public void setReleased(boolean released) {
    this.released = released;
  }

  @Override
  public String toString() {
    return id != null ? id.toString() : nullToEmpty(name);
  }

}
