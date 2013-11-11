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

import com.impossibl.postgres.utils.guava.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

class LargeObject {

  protected static final int INV_READ   = 0x00040000;
  protected static final int INV_WRITE  = 0x00020000;

  protected static final int SEEK_SET = 0;
  protected static final int SEEK_CUR = 1;
  protected static final int SEEK_END = 2;

  int oid;
  int fd;
  PGConnectionImpl connection;

  static LargeObject open(PGConnectionImpl connection, int oid) throws SQLException {
    int fd = open(connection, oid, INV_READ | INV_WRITE);
    if (fd == -1) {
      throw new SQLException("Unable to open large object");
    }

    if (connection.isServerMinimumVersion(9, 3))
      return new LargeObject64(connection, oid, fd);
    else
      return new LargeObject(connection, oid, fd);
  }

  LargeObject(PGConnectionImpl connection, int oid, int fd) {
    super();
    this.oid = oid;
    this.fd = fd;
    this.connection = connection;
  }

  LargeObject dup() throws SQLException {
    return open(connection, oid);
  }

  static int creat(PGConnectionImpl conn, int mode) throws SQLException {
    return conn.executeForFirstResultValue("select lo_creat($1)", true, Integer.class, mode);
  }

  static int open(PGConnectionImpl conn, int oid, int access) throws SQLException {
    return conn.executeForFirstResultValue("select lo_open($1,$2)", true, Integer.class, oid, access);
  }

  static int unlink(PGConnectionImpl conn, int oid) throws SQLException {
    return conn.executeForFirstResultValue("select lo_unlink($1)", true, Integer.class, oid);
  }

  int close() throws SQLException {
    return connection.executeForFirstResultValue("select lo_close($1)", true, Integer.class, fd);
  }

  long lseek(long offset, int whence) throws SQLException {
    return connection.executeForFirstResultValue("select lo_lseek($1,$2,$3)", true, Integer.class, fd, (int) offset, whence);
  }

  long tell() throws SQLException {
    return connection.executeForFirstResultValue("select lo_tell($1)", true, Integer.class, fd);
  }

  byte[] read(long len) throws SQLException {
    InputStream data = connection.executeForFirstResultValue("select loread($1,$2)", true, InputStream.class, fd, (int) len);
    try {
      return ByteStreams.toByteArray(data);
    }
    catch (IOException e) {
      throw new SQLException(e);
    }
  }

  int write(byte[] data, int off, int len) throws SQLException {

    InputStream dataIn = new ByteArrayInputStream(data, off, len);

    return connection.executeForFirstResultValue("select lowrite($1,$2)", true, Integer.class, fd, dataIn);
  }

  int truncate(long len) throws SQLException {
    return connection.executeForFirstResultValue("select lo_truncate($1,$2)", true, Integer.class, fd, (int) len);
  }

}
