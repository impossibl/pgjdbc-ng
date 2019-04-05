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
package com.impossibl.postgres.api.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Public API for PGConnection
 */
public interface PGConnection extends Connection {

  /**
   * Checks the minimum server version
   * @param major The major release
   * @param minor The minor release
   * @return <code>True</code> if the server is minimum the specified version, otherwise <code>false</code>.
   */
  boolean isServerMinimumVersion(int major, int minor);

  /**
   * Adds a filtered asynchronous notification listener to this connection
   *
   * @param name
   *          Name of listener
   * @param channelNameFilter
   *          Channel name based notification filter (Regular Expression)
   * @param listener
   *          Notification listener
   */
  void addNotificationListener(String name, String channelNameFilter, PGNotificationListener listener);

  /**
   * Adds an, unnamed, filtered, asynchronous notification listener to this
   * connection
   *
   * @param channelNameFilter
   *          Channel name based notification filter (Regular Expression)
   * @param listener
   *          Notification listener
   */
  void addNotificationListener(String channelNameFilter, PGNotificationListener listener);

  /**
   * Adds an, unnamed, unfiltered, asynchronous notification listener to this
   * connection
   *
   * @param listener
   *          Notification listener
   */
  void addNotificationListener(PGNotificationListener listener);

  /**
   * Removes a named notification listener
   * 
   * @param name
   *          Name of listener to remove
   */
  void removeNotificationListener(String name);

  /**
   * Removes a notification listener
   * 
   * @param listener
   *          Listener instance to remove
   */
  void removeNotificationListener(PGNotificationListener listener);

  /**
   * Set strict mode
   * @param v The value
   */
  void setStrictMode(boolean v);

  /**
   * Is strict mode
   * @return The value
   */
  boolean isStrictMode();

  /**
   * Set the default fetch size
   * @param v The value
   */
  void setDefaultFetchSize(Integer v);

  /**
   * Get the default fetch size
   * @return The value
   */
  Integer getDefaultFetchSize();


  /**
   * Resolves a name to an SQLType suitable for passing
   * to any method that accepts one.
   *
   * As the method queries the server for the type information
   * any type name acceptable to the server is allowed (i.e.
   * qualified, unqualified, quoted, etc.)
   *
   * Using this method allows the resolution of a name to
   * a type to be re-used; speeding up performance in
   * cases where named types are used.
   *
   * NOTE: The possibility of DDL changing the name to type
   * mapping means named types are always looked up when used.
   * This ensures that the correct type is used always. When
   * resolving a named type using this method it is only valid
   * as long as no commands are issued that invalidate it (e.g.
   * dropping and re-adding a type or extension).
   *
   * @param name Name of type to resolve (anything acceptable to the server)
   * @return Type instance representing the current name to type mapping.
   */
  PGAnyType resolveType(String name) throws SQLException;


  /**
   * Allows issuing PostgreSQL's COPY command providing an {@link InputStream}
   * stream to read from, instead of relying on {@link System#in}.
   *
   * @param sql SQL text for a valid <code>COPY ... FROM STDIN</code> command.
   * @param inputStream {@link InputStream} containing data in <code>COPY</code> format.
   * @throws SQLException If an error occurs during the copy operation or if an alternate command is provided.
   */
  void copyFrom(String sql, InputStream inputStream) throws SQLException;


  /**
   * Allows issuing PostgreSQL's COPY command providing an {@link OutputStream}
   * stream to write to, instead of relying on {@link System#out}.
   *
   * @param sql SQL text for a valid <code>COPY ... TO STDOUT</code> command.
   * @param outputStream {@link OutputStream} to write data in <code>COPY</code> format.
   * @throws SQLException If an error occurs during the copy operation or if an alternate command is provided.
   */
  void copyTo(String sql, OutputStream outputStream) throws SQLException;

}
