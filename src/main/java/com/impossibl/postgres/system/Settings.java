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

import com.impossibl.postgres.protocol.ssl.ConsolePasswordCallbackHandler;
import com.impossibl.postgres.protocol.ssl.SSLMode;



public class Settings {

  public static final String DATABASE = "database";
  public static final String DATABASE_URL = "databaseUrl";

  public static final String CONNECTION_READONLY = "readOnly";

  public static final String PARSED_SQL_CACHE = "parsedSqlCacheSize";
  public static final int PARSED_SQL_CACHE_DEFAULT = 250;

  public static final String PREPARED_STATEMENT_CACHE = "preparedStatementCacheSize";
  public static final int PREPARED_STATEMENT_CACHE_DEFAULT = 50;

  public static final String CLIENT_ENCODING = "client_encoding";
  public static final String APPLICATION_NAME = "application_name";

  public static final String CREDENTIALS_USERNAME = "user";
  public static final String CREDENTIALS_PASSWORD = "password";

  public static final String FIELD_VARYING_LENGTH_MAX       = "field.varying.length.max";
  public static final String FIELD_MONEY_FRACTIONAL_DIGITS  = "field.money.fractionalDigits";
  public static final String FIELD_DATETIME_FORMAT_CLASS    = "field.datetime.format";

  public static final String STANDARD_CONFORMING_STRINGS = "standard_conforming_strings";

  public static final String BLOB_TYPE = "blob.type";
  public static final String BLOB_TYPE_DEFAULT = "blobid";

  public static final String CLOB_TYPE = "clob.type";
  public static final String CLOB_TYPE_DEFAULT = "clobid";

  public static final String PARAMETER_STREAM_THRESHOLD = "parameter.stream.threshold";
  public static final int PARAMETER_STREAM_THRESHOLD_DEFAULT = 500 * 1024;

  public static final String SSL_MODE = "ssl.mode";
  public static final SSLMode SSL_MODE_DEFAULT = SSLMode.Disable;

  public static final String SSL_CERT_FILE = "ssl.cert.file";
  public static final String SSL_CERT_FILE_DEFAULT = "postgresql.crt";

  public static final String SSL_KEY_FILE = "ssl.key.file";
  public static final String SSL_KEY_FILE_DEFAULT = "postgresql.pk8";

  public static final String SSL_PASSWORD = "ssl.password";

  public static final String SSL_PASSWORD_CALLBACK = "ssl.password.callback";
  public static final String SSL_PASSWORD_CALLBACK_DEFAULT = ConsolePasswordCallbackHandler.class.getName();

  public static final String SSL_ROOT_CERT_FILE = "ssl.root.cert.file";
  public static final String SSL_ROOT_CERT_FILE_DEFAULT = "root.crt";

}
