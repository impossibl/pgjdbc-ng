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

import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.ssl.ConsolePasswordCallbackHandler;
import com.impossibl.postgres.protocol.ssl.SSLMode;



public class Settings {

  public static final String DATABASE = "database";
  public static final String DATABASE_URL = "databaseUrl";

  public static final String CONNECTION_READONLY = "readOnly";

  public static final String PARSED_SQL_CACHE_SIZE = "parsedSqlCacheSize";
  public static final int PARSED_SQL_CACHE_SIZE_DEFAULT = 250;

  public static final String PREPARED_STATEMENT_CACHE_SIZE = "preparedStatementCacheSize";
  public static final int PREPARED_STATEMENT_CACHE_SIZE_DEFAULT = 50;
  public static final String PREPARED_STATEMENT_CACHE_THRESHOLD = "preparedStatementCacheThreshold";
  public static final int PREPARED_STATEMENT_CACHE_THRESHOLD_DEFAULT = 0;

  public static final String DESCRIPTION_CACHE_SIZE = "descriptionCacheSize";
  public static final int DESCRIPTION_CACHE_SIZE_DEFAULT = 200;

  public static final String CLIENT_ENCODING = "client_encoding";
  public static final String CLIENT_ENCODING_DEFAULT = "UTF8";

  public static final String APPLICATION_NAME = "application_name";
  public static final String APPLICATION_NAME_DEFAULT = "PG-JDBC (NG)";

  public static final String CREDENTIALS_USERNAME = "user";
  public static final String CREDENTIALS_PASSWORD = "password";

  public static final String FIELD_VARYING_LENGTH_MAX       = "field.varying.length.max";
  public static final String FIELD_MONEY_FRACTIONAL_DIGITS  = "field.money.fractionalDigits";
  public static final String FIELD_DATETIME_FORMAT_CLASS    = "field.datetime.format";

  public static final String FIELD_FORMAT_PREF              = "field.format.preference";
  public static final String FIELD_FORMAT_PREF_DEFAULT      = FieldFormat.Binary.name();
  public static final String PARAM_FORMAT_PREF              = "param.format.preference";
  public static final String PARAM_FORMAT_PREF_DEFAULT      = FieldFormat.Binary.name();

  public static final String STANDARD_CONFORMING_STRINGS = "standard_conforming_strings";

  public static final String BLOB_TYPE = "blob.type";
  public static final String BLOB_TYPE_DEFAULT = "blobid";

  public static final String CLOB_TYPE = "clob.type";
  public static final String CLOB_TYPE_DEFAULT = "clobid";

  public static final String SSL_MODE = "sslMode";
  public static final SSLMode SSL_MODE_DEFAULT = SSLMode.Disable;

  public static final String SSL_CERT_FILE = "sslCertificateFile";
  public static final String SSL_CERT_FILE_DEFAULT = "postgresql.crt";

  public static final String SSL_KEY_FILE = "sslKeyFile";
  public static final String SSL_KEY_FILE_DEFAULT = "postgresql.pk8";

  public static final String SSL_PASSWORD = "sslPassword";

  public static final String SSL_PASSWORD_CALLBACK = "sslPasswordCallback";
  public static final String SSL_PASSWORD_CALLBACK_DEFAULT = ConsolePasswordCallbackHandler.class.getName();

  public static final String SSL_ROOT_CERT_FILE = "sslRootCertificateFile";
  public static final String SSL_ROOT_CERT_FILE_DEFAULT = "root.crt";

  public static final String NETWORK_TIMEOUT = "networkTimeout";
  public static final int NETWORK_TIMEOUT_DEFAULT = 0;

  public static final String STRICT_MODE = "strictMode";
  public static final boolean STRICT_MODE_DEFAULT = false;

  public static final String DEFAULT_FETCH_SIZE = "defaultFetchSize";
  public static final int DEFAULT_FETCH_SIZE_DEFAULT = 0;

  public static final String RECEIVE_BUFFER_SIZE = "receiveBufferSize";
  public static final int RECEIVE_BUFFER_SIZE_DEFAULT = -1;

  public static final String SEND_BUFFER_SIZE = "sendBufferSize";
  public static final int SEND_BUFFER_SIZE_DEFAULT = -1;

  public static final String ALLOCATOR = "allocator.pooled";
  public static final boolean ALLOCATOR_DEFAULT = true;

  public static final String MAX_MESSAGE_SIZE = "protocol.message.max";
  public static final int MAX_MESSAGE_SIZE_DEFAULT = 15 * 1024 * 1024;

  public static final String PROTOCOL_TRACE = "protocol.trace";
  public static final boolean PROTOCOL_TRACE_DEFAULT = false;

  public static final String PROTOCOL_SOCKET_IO = "protocol.socket.io";
  public static final String PROTOCOL_SOCKET_IO_DEFAULT = "nio";

  public static final String PROTOCOL_SOCKET_IO_THREADS = "protocol.socket.io.threads";
  public static final int PROTOCOL_SOCKET_IO_THREADS_DEFAULT = 3;

  public static final String SQL_TRACE = "sql.trace";
  public static final boolean SQL_TRACE_DEFAULT = false;

}
