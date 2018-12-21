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

import com.impossibl.postgres.datetime.DateTimeFormat;
import com.impossibl.postgres.datetime.ISODateFormat;
import com.impossibl.postgres.datetime.ISOTimeFormat;
import com.impossibl.postgres.datetime.ISOTimestampFormat;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.ExecuteResult;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.PrepareResult;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.QueryResult;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.ServerConnection;
import com.impossibl.postgres.protocol.ServerConnectionFactory;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.system.tables.Table;
import com.impossibl.postgres.system.tables.Tables;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.ByteBufs;
import com.impossibl.postgres.utils.Locales;
import com.impossibl.postgres.utils.Timer;

import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;
import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.system.Settings.STANDARD_CONFORMING_STRINGS;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.WARNING;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;


public class BasicContext extends AbstractContext {

  private static final long INTERNAL_QUERY_TIMEOUT = SECONDS.toMillis(60);

  private static final Logger logger = Logger.getLogger(BasicContext.class.getName());

  private static class QueryDescription {

    String name;
    Type[] parameterTypes;
    ResultField[] resultFields;

    QueryDescription(String name, Type[] parameterTypes, ResultField[] resultFields) {
      this.name = name;
      this.parameterTypes = parameterTypes;
      this.resultFields = resultFields;
    }
  }

  private static class NotificationKey {

    private String name;
    private Pattern channelNameFilter;

    NotificationKey(String name, Pattern channelNameFilter) {
      this.name = name;
      this.channelNameFilter = channelNameFilter;
    }

    String getName() {
      return name;
    }

  }


  protected Registry registry;
  protected Map<String, Class<?>> typeMap;
  protected Charset charset;
  private TimeZone timeZone;
  private DateTimeFormat dateFormatter;
  private DateTimeFormat timeFormatter;
  private DateTimeFormat timestampFormatter;
  private NumberFormat integerFormatter;
  private DecimalFormat decimalFormatter;
  private DecimalFormat currencyFormatter;
  protected Properties settings;
  private Version serverVersion;
  private KeyData keyData;
  protected ServerConnection serverConnection;
  protected Map<NotificationKey, NotificationListener> notificationListeners;
  private Map<String, QueryDescription> utilQueries;


  public BasicContext(SocketAddress address, Properties settings, Map<String, Class<?>> typeMap) throws IOException, NoticeException {
    this.typeMap = new HashMap<>(typeMap);
    this.settings = settings;
    this.charset = UTF_8;
    this.timeZone = TimeZone.getTimeZone("UTC");
    this.dateFormatter = new ISODateFormat();
    this.timeFormatter = new ISOTimeFormat();
    this.timestampFormatter = new ISOTimestampFormat();
    this.notificationListeners = new ConcurrentHashMap<>();
    this.registry = new Registry(this);
    this.serverConnection = ServerConnectionFactory.getDefault().connect(address, this);
    this.utilQueries = new HashMap<>();
  }

  protected void shutdown() {
    serverConnection.shutdown().syncUninterruptibly();
  }

  public Version getServerVersion() {
    return serverVersion;
  }

  public ByteBufAllocator getAllocator() {
    return serverConnection.getAllocator();
  }

  public ServerConnection getServerConnection() {
    return serverConnection;
  }

  @Override
  public Registry getRegistry() {
    return registry;
  }

  @Override
  public RequestExecutor getRequestExecutor() {
    return serverConnection.getRequestExecutor();
  }

  @Override
  public Object getSetting(String name) {
    Object value = settings.get(name);
    if (value != null)
      return value;
    return super.getSetting(name);
  }

  @Override
  public Map<String, Class<?>> getCustomTypeMap() {
    return typeMap;
  }

  @Override
  public Charset getCharset() {
    return charset;
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public KeyData getKeyData() {
    return keyData;
  }

  @Override
  public DateTimeFormat getDateFormatter() {
    return dateFormatter;
  }

  @Override
  public DateTimeFormat getTimeFormatter() {
    return timeFormatter;
  }

  @Override
  public DateTimeFormat getTimestampFormatter() {
    return timestampFormatter;
  }

  public NumberFormat getIntegerFormatter() {
    return integerFormatter;
  }

  @Override
  public DecimalFormat getDecimalFormatter() {
    return decimalFormatter;
  }

  @Override
  public DecimalFormat getCurrencyFormatter() {
    return currencyFormatter;
  }

  protected void init() throws IOException, NoticeException {

    integerFormatter = NumberFormat.getIntegerInstance();
    integerFormatter.setGroupingUsed(false);

    decimalFormatter = (DecimalFormat) DecimalFormat.getNumberInstance();
    decimalFormatter.setGroupingUsed(false);

    currencyFormatter = (DecimalFormat) DecimalFormat.getCurrencyInstance();
    currencyFormatter.setGroupingUsed(false);

    loadTypes();

    prepareRefreshTypeQueries();

    loadLocale();
  }

  private void loadLocale() throws IOException, NoticeException {

    try (ResultBatch resultBatch =
        queryBatch("SELECT name, setting FROM pg_settings WHERE name IN ('lc_numeric', 'lc_time')", INTERNAL_QUERY_TIMEOUT)) {

      for (ResultBatch.Row row : resultBatch) {

        String localeSpec = row.getField(1, this, String.class);

        switch (localeSpec.toUpperCase(Locale.US)) {
          case "C":
          case "POSIX":
            localeSpec = "en_US";
            break;
        }

        localeSpec = Locales.getJavaCompatibleLocale(localeSpec);

        String[] localeIds = localeSpec.split("[_.]");

        switch (row.getField(0, this, String.class)) {
          case "lc_numeric":

            Locale numLocale = new Locale.Builder().setLanguageTag(localeIds[0]).setRegion(localeIds[1]).build();

            integerFormatter = NumberFormat.getIntegerInstance(numLocale);
            integerFormatter.setParseIntegerOnly(true);
            integerFormatter.setGroupingUsed(false);

            decimalFormatter = (DecimalFormat) DecimalFormat.getNumberInstance(numLocale);
            decimalFormatter.setParseBigDecimal(true);
            decimalFormatter.setGroupingUsed(false);

            currencyFormatter = (DecimalFormat) NumberFormat.getCurrencyInstance(numLocale);
            currencyFormatter.setParseBigDecimal(true);
            currencyFormatter.setGroupingUsed(false);
            break;

          case "lc_time":
            // TODO setup time locale
            // Locale timeLocale = new Locale.Builder().setLanguageTag(localeIds[0]).setRegion(localeIds[1]).build();
        }

      }
    }

  }

  private void loadTypes() throws IOException, NoticeException {

    Timer timer = new Timer();

    //Load types
    String typeSQL = PgType.INSTANCE.getSQL(serverVersion);
    List<PgType.Row> pgTypes = queryTable(typeSQL, PgType.INSTANCE);

    //Load attributes
    String attrsSQL = PgAttribute.INSTANCE.getSQL(serverVersion);
    List<PgAttribute.Row> pgAttrs = queryTable(attrsSQL, PgAttribute.INSTANCE);

    //Load procs
    String procsSQL = PgProc.INSTANCE.getSQL(serverVersion);
    List<PgProc.Row> pgProcs = queryTable(procsSQL, PgProc.INSTANCE);

    logger.fine("query time: " + timer.getLap() + "ms");

    //Update the registry with known types
    registry.update(pgTypes, pgAttrs, pgProcs);

    logger.fine("load time: " + timer.getLap() + "ms");
  }

  private void prepareRefreshTypeQueries() throws IOException, NoticeException {

    prepareUtilQuery("refresh-type", PgType.INSTANCE.getSQL(serverVersion) + " where t.oid = $1");

    prepareUtilQuery("refresh-type-attrs", PgAttribute.INSTANCE.getSQL(serverVersion) + " and a.attrelid = $1", "int4");

    prepareUtilQuery("refresh-types", PgType.INSTANCE.getSQL(serverVersion) + " where t.oid > $1", "int4");

    prepareUtilQuery("refresh-types-attrs", PgAttribute.INSTANCE.getSQL(serverVersion) + " and a.attrelid = any( $1 )", "int4[]");

    prepareUtilQuery("refresh-reltype", PgType.INSTANCE.getSQL(serverVersion) + " where t.typrelid = $1", "int4");

  }

  @Override
  public void refreshType(int typeId) {

    int latestKnownTypeId = registry.getLatestKnownTypeId();
    if (latestKnownTypeId >= typeId) {
      //Refresh this specific type
      refreshSpecificType(typeId);
    }
    else {
      //Load all new types we haven't seen
      refreshTypes(latestKnownTypeId);
    }

  }

  private void refreshSpecificType(int typeId) {

    try {

      //Load types
      List<PgType.Row> pgTypes = queryTable("@refresh-type", PgType.INSTANCE, typeId);

      if (pgTypes.isEmpty()) {
        return;
      }

      //Load attributes
      List<PgAttribute.Row> pgAttrs = queryTable("@refresh-type-attrs", PgAttribute.INSTANCE, pgTypes.get(0).getRelationId());

      registry.update(pgTypes, pgAttrs, emptyList());
    }
    catch (IOException | NoticeException e) {
      //Ignore errors
    }

  }

  private void refreshTypes(int latestTypeId) {

    try {

      //Load types
      List<PgType.Row> pgTypes = queryTable("@refresh-types", PgType.INSTANCE, latestTypeId);

      if (pgTypes.isEmpty()) {
        return;
      }

      Integer[] typeIds = new Integer[pgTypes.size()];
      for (int c = 0; c < pgTypes.size(); ++c)
        typeIds[c] = pgTypes.get(c).getRelationId();

      //Load attributes
      List<PgAttribute.Row> pgAttrs = queryTable("@refresh-types-attrs", PgAttribute.INSTANCE, (Object) typeIds);

      registry.update(pgTypes, pgAttrs, emptyList());
    }
    catch (IOException | NoticeException e) {
      logger.log(WARNING, "Error refreshing types", e);
    }

  }

  @Override
  public void refreshRelationType(int relationId) {

    try {

      //Load types
      List<PgType.Row> pgTypes = queryTable("@refresh-reltype", PgType.INSTANCE, relationId);

      if (pgTypes.isEmpty()) {
        return;
      }

      //Load attributes
      List<PgAttribute.Row> pgAttrs = queryTable("@refresh-type-attrs", PgAttribute.INSTANCE, relationId);

      registry.update(pgTypes, pgAttrs, emptyList());
    }
    catch (IOException | NoticeException e) {
      //Ignore errors
    }

  }

  public boolean isUtilQueryPrepared(String name) {
    return utilQueries.containsKey(name);
  }

  public void prepareUtilQuery(String name, String sql, String... parameterTypeNames) throws IOException, NoticeException {

    Type[] parameterTypes = new Type[parameterTypeNames.length];
    for (int parameterIdx = 0; parameterIdx < parameterTypes.length; ++parameterIdx) {
      parameterTypes[parameterIdx] = registry.loadType(parameterTypeNames[parameterIdx]);
    }

    prepareUtilQuery(name, sql, parameterTypes);
  }

  private void prepareUtilQuery(String name, String sql, Type[] parameterTypes) throws IOException, NoticeException {

    PrepareResult handler = new PrepareResult();

    serverConnection.getRequestExecutor().prepare(name, sql, parameterTypes, handler);

    handler.await(INTERNAL_QUERY_TIMEOUT, MILLISECONDS);

    QueryDescription desc = new QueryDescription(name, handler.getDescribedParameterTypes(this), handler.getDescribedResultFields());
    utilQueries.put(name, desc);
  }

  private QueryDescription prepareQuery(String queryTxt) throws NoticeException, IOException {

    if (queryTxt.charAt(0) == '@') {
      QueryDescription util = utilQueries.get(queryTxt.substring(1));
      if (util == null) {
        throw new IOException("invalid utility query");
      }
      return util;
    }

    PrepareResult handler = new PrepareResult();

    serverConnection.getRequestExecutor().prepare(null, queryTxt, EMPTY_TYPES, handler);

    handler.await(INTERNAL_QUERY_TIMEOUT, MILLISECONDS);

    return new QueryDescription(null, handler.getDescribedParameterTypes(this), handler.getDescribedResultFields());
  }

  private <R extends Table.Row, T extends Table<R>> List<R> queryTable(String queryTxt, T table, Object... params) throws IOException, NoticeException {


    try (ResultBatch resultBatch = queryBatchPrepared(queryTxt, params, INTERNAL_QUERY_TIMEOUT)) {
      return Tables.convertRows(this, table, resultBatch);
    }

  }

  public void query(String queryTxt, long timeout) throws IOException, NoticeException {

    if (queryTxt.charAt(0) == '@') {

      QueryDescription pq = prepareQuery(queryTxt);

      queryBatchPrepared(pq.name, EMPTY_FORMATS, EMPTY_BUFFERS, pq.resultFields, timeout).close();
    }
    else {

      QueryResult handler = new QueryResult();

      serverConnection.getRequestExecutor().query(queryTxt, handler);

      handler.await(timeout, MILLISECONDS);

      handler.getBatch().close();
    }

  }

  protected String queryString(String queryTxt, long timeout) throws IOException, NoticeException {

    try (ResultBatch resultBatch = queryBatch(queryTxt, timeout)) {
      String val = resultBatch.getRow(0).getField(0, this, String.class);
      return nullToEmpty(val);
    }

  }

  /**
   * Queries for a single (the first) result batch. The batch must be released.
   */
  protected ResultBatch queryBatch(String queryTxt, long timeout) throws IOException, NoticeException {

    if (queryTxt.charAt(0) == '@') {

      QueryDescription pq = prepareQuery(queryTxt);

      return queryBatchPrepared(pq.name, EMPTY_FORMATS, EMPTY_BUFFERS, pq.resultFields, timeout);
    }
    else {

      QueryResult handler = new QueryResult();

      serverConnection.getRequestExecutor().query(queryTxt, handler);

      handler.await(timeout, MILLISECONDS);

      return handler.getBatch();
    }

  }

  /**
   * Queries a single result batch (the first) via a parameterized query. The batch must be released.
   */
  protected ResultBatch queryBatchPrepared(String queryTxt, Object[] paramValues, long timeout) throws IOException, NoticeException {

    QueryDescription pq = prepareQuery(queryTxt);

    FieldFormat[] paramFormats = EMPTY_FORMATS;
    ByteBuf[] paramBuffers = EMPTY_BUFFERS;
    try {

      if (paramValues.length != 0) {

        paramFormats = new FieldFormat[paramValues.length];
        paramBuffers = new ByteBuf[paramValues.length];

        for (int paramIdx = 0; paramIdx < paramValues.length; ++paramIdx) {
          Type paramType = pq.parameterTypes[paramIdx];
          Object paramValue = paramValues[paramIdx];
          if (paramValue == null) continue;

          FieldFormat paramFormat = paramType.getParameterFormat();
          paramFormats[paramIdx] = paramFormat;

          switch (paramFormat) {
            case Text: {
              StringBuilder out = new StringBuilder();
              paramType.getTextCodec().getEncoder().encode(this, paramType, paramValue, null, out);
              paramBuffers[paramIdx] = ByteBufUtil.writeUtf8(getAllocator(), out);
            }
            break;

            case Binary: {
              ByteBuf out = getAllocator().buffer();
              paramType.getBinaryCodec().getEncoder().encode(this, paramType, paramValue, null, out);
              paramBuffers[paramIdx] = out;
            }
            break;
          }
        }

      }

      return queryBatchPrepared(pq.name, paramFormats, paramBuffers, pq.resultFields, timeout);
    }
    finally {
      ByteBufs.releaseAll(paramBuffers);
    }
  }

  /**
   * Queries a single result batch (the first) via a parameterized query. The batch must be released.
   */
  protected ResultBatch queryBatchPrepared(String queryTxt,
                                           FieldFormatRef[] paramFormats, ByteBuf[] paramBuffers,
                                           long timeout) throws IOException, NoticeException {

    QueryDescription pq = prepareQuery(queryTxt);

    return queryBatchPrepared(pq.name, paramFormats, paramBuffers, pq.resultFields, timeout);
  }

  /**
   * Queries a single result batch (the first) via a parameterized query. The batch must be released.
   */
  private ResultBatch queryBatchPrepared(String statementName,
                                         FieldFormatRef[] paramFormats, ByteBuf[] paramBuffers,
                                         ResultField[] resultFields, long timeout) throws IOException, NoticeException {

    ExecuteResult handler = new ExecuteResult(resultFields);

    serverConnection.getRequestExecutor()
        .execute(null, statementName, paramFormats, paramBuffers, resultFields, 0, handler);

    handler.await(timeout, MILLISECONDS);

    return handler.getBatch();
  }

  public void setKeyData(int processId, int secretKey) {
    keyData = new KeyData(processId, secretKey);
  }

  public void updateSystemParameter(String name, String value) {

    logger.config("system parameter: " + name + "=" + value);

    switch (name) {

      case "server_version":

        serverVersion = Version.parse(value);
        break;

      case "DateStyle":

        String[] parsedDateStyle = DateStyle.parse(value);

        if (parsedDateStyle == null) {
          logger.warning("Invalid DateStyle encountered");
        }
        else {

          dateFormatter = DateStyle.getDateFormatter(parsedDateStyle);
          if (dateFormatter == null) {
            logger.warning("Unknown Date format, reverting to default");
            dateFormatter = new ISODateFormat();
          }

          timeFormatter = DateStyle.getTimeFormatter(parsedDateStyle);
          if (timeFormatter == null) {
            logger.warning("Unknown Time format, reverting to default");
            timeFormatter = new ISOTimeFormat();
          }

          timestampFormatter = DateStyle.getTimestampFormatter(parsedDateStyle);
          if (timestampFormatter == null) {
            logger.warning("Unknown Timestamp format, reverting to default");
            timestampFormatter = new ISOTimestampFormat();
          }
        }
        break;

      case "TimeZone":
        if (value.contains("+")) {
          value = value.replace('+', '-');
        }
        else {
          value = value.replace('-', '+');
        }

        timeZone = TimeZone.getTimeZone(value);
        break;

      case "integer_datetimes":

        settings.put(FIELD_DATETIME_FORMAT_CLASS, Integer.class);
        break;

      case "client_encoding":

        charset = Charset.forName(value);
        break;

      case STANDARD_CONFORMING_STRINGS:

        settings.put(STANDARD_CONFORMING_STRINGS, value.equals("on"));
        break;

      default:
        break;
    }

  }

  protected void addNotificationListener(String name, String channelNameFilter, NotificationListener listener) {

    name = nullToEmpty(name);
    channelNameFilter = channelNameFilter != null ? channelNameFilter : ".*";

    Pattern channelNameFilterPattern = Pattern.compile(channelNameFilter);

    NotificationKey key = new NotificationKey(name, channelNameFilterPattern);

    notificationListeners.put(key, listener);
  }

  protected synchronized void removeNotificationListener(NotificationListener listener) {

    Iterator<Map.Entry<NotificationKey, NotificationListener>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, NotificationListener> entry = iter.next();

      NotificationListener iterListener = entry.getValue();
      if (iterListener == null || iterListener.equals(listener)) {

        iter.remove();
      }

    }
  }

  public synchronized void removeNotificationListener(String listenerName) {

    Iterator<Map.Entry<NotificationKey, NotificationListener>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, NotificationListener> entry = iter.next();

      String iterListenerName = entry.getKey().name;
      NotificationListener iterListener = entry.getValue();
      if (iterListenerName.equals(listenerName) || iterListener == null) {

        iter.remove();
      }

    }
  }

  public synchronized void reportNotification(int processId, String channelName, String payload) {

    for (Map.Entry<NotificationKey, NotificationListener> entry : notificationListeners.entrySet()) {

      NotificationListener listener = entry.getValue();
      if (entry.getKey().channelNameFilter.matcher(channelName).matches()) {

        listener.notification(processId, channelName, payload);
      }

    }

  }

  @Override
  public Context unwrap() {
    return this;
  }

}
