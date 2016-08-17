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

import java.sql.SQLException;

class LargeObject64 extends LargeObject {

  LargeObject64(PGDirectConnection connection, int oid, int fd) throws SQLException {
    super(connection, oid, fd);

    ensurePrepared(connection, "lo.lseek64", "select lo_lseek64($1,$2,$3)", "int4", "int8", "int4");
    ensurePrepared(connection, "lo.tell64", "select lo_tell64($1)", "int4");
    ensurePrepared(connection, "lo.truncate64", "select lo_truncate64($1,$2)", "int4", "int8");
  }

  @Override
  long lseek(long offset, int whence) throws SQLException {
    return connection.executeForValue("@lo.lseek64", Long.class, fd, offset, whence);
  }

  @Override
  long tell() throws SQLException {
    return connection.executeForValue("@lo.tell64", Long.class, fd);
  }

  @Override
  int truncate(long len) throws SQLException {
    return connection.executeForValue("@lo.truncate64", Integer.class, fd, len);
  }

}
