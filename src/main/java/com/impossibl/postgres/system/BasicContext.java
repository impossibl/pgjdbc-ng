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
import com.impossibl.postgres.protocol.BindExecCommand;
import com.impossibl.postgres.protocol.PrepareCommand;
import com.impossibl.postgres.protocol.Protocol;
import com.impossibl.postgres.protocol.QueryCommand;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.v30.ProtocolFactoryImpl;
import com.impossibl.postgres.system.tables.PgAttribute;
import com.impossibl.postgres.system.tables.PgProc;
import com.impossibl.postgres.system.tables.PgType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.types.Type.Category;
import com.impossibl.postgres.utils.Converter;
import com.impossibl.postgres.utils.Timer;

import static com.impossibl.postgres.system.Settings.FIELD_DATETIME_FORMAT_CLASS;
import static com.impossibl.postgres.system.Settings.STANDARD_CONFORMING_STRINGS;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.logging.Level.WARNING;


public class BasicContext implements Context {

  private static final Logger logger = Logger.getLogger(BasicContext.class.getName());

  private static class PreparedQuery {

    String name;
    List<Type> parameterTypes;
    List<ResultField> resultFields;

    PreparedQuery(String name, List<Type> parameterTypes, List<ResultField> resultFields) {
      this.name = name;
      this.parameterTypes = parameterTypes;
      this.resultFields = resultFields;
    }
  }

  private static class NotificationKey {

    public String name;
    public Pattern channelNameFilter;

    NotificationKey(String name, Pattern channelNameFilter) {
      this.name = name;
      this.channelNameFilter = channelNameFilter;
    }

  }


  protected Registry registry;
  protected Map<String, Class<?>> targetTypeMap;
  protected Charset charset;
  protected TimeZone timeZone;
  protected DateTimeFormat dateFormatter;
  protected DateTimeFormat timeFormatter;
  protected DateTimeFormat timestampFormatter;
  protected Properties settings;
  protected Version serverVersion;
  protected KeyData keyData;
  protected Protocol protocol;
  protected Map<NotificationKey, WeakReference<NotificationListener>> notificationListeners;
  protected Map<String, PreparedQuery> utilQueries;


  public BasicContext(SocketAddress address, Properties settings, Map<String, Class<?>> targetTypeMap) throws IOException, NoticeException {
    this.targetTypeMap = new HashMap<>(targetTypeMap);
    this.settings = settings;
    this.charset = UTF_8;
    this.timeZone = TimeZone.getTimeZone("UTC");
    this.dateFormatter = new ISODateFormat();
    this.timeFormatter = new ISOTimeFormat();
    this.timestampFormatter = new ISOTimestampFormat();
    this.notificationListeners = new ConcurrentHashMap<>();
    this.registry = new Registry(this);
    this.protocol = new ProtocolFactoryImpl().connect(address, this);
    this.utilQueries = new HashMap<>();
  }

  protected void shutdown() {
    protocol.shutdown();
  }

  public Version getServerVersion() {
    return serverVersion;
  }

  public void setServerVersion(Version serverVersion) {
    this.serverVersion = serverVersion;
  }

  @Override
  public Registry getRegistry() {
    return registry;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public Object getSetting(String name) {
    return settings.get(name);
  }

  @Override
  public <T> T getSetting(String name, Class<T> type) {
    return type.cast(settings.get(name));
  }

  public <T> T getSetting(String name, Converter<T> converter) {
    return converter.apply(settings.get(name));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getSetting(String name, T defaultValue) {
    Object val = settings.get(name);
    if (val == null)
      return defaultValue;
    if ((defaultValue.getClass() == int.class || defaultValue.getClass() == Integer.class) && val instanceof String) {
      return (T) defaultValue.getClass().cast(Integer.valueOf((String) val));
    }
    if ((defaultValue.getClass() == long.class || defaultValue.getClass() == Long.class) && val instanceof String) {
      return (T) defaultValue.getClass().cast(Long.valueOf((String) val));
    }
    if ((defaultValue.getClass() == boolean.class || defaultValue.getClass() == Boolean.class) && val instanceof String) {
      return (T) defaultValue.getClass().cast(Boolean.valueOf((String) val));
    }
    return (T) defaultValue.getClass().cast(val);
  }

  @Override
  public boolean isSettingEnabled(String name) {
    Object val = getSetting(name);
    if (val instanceof String)
      return ((String)val).equalsIgnoreCase("on");
    if (val instanceof Boolean)
      return (Boolean) val;
    return false;
  }

  @Override
  public Class<?> lookupInstanceType(Type type) {

    Class<?> cls = targetTypeMap.get(type.getName());
    if (cls == null) {
      if (type.getCategory() == Category.Array)
        return Object[].class;
      else
        cls = HashMap.class;
    }

    return cls;
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

  protected void init() throws IOException, NoticeException {

    loadTypes();

    prepareRefreshTypeQueries();
  }

  private void loadTypes() throws IOException, NoticeException {

    Timer timer = new Timer();

    //Load types
    String typeSQL = PgType.INSTANCE.getSQL(serverVersion);
    List<PgType.Row> pgTypes = queryResults(typeSQL, PgType.Row.class);

    //Load attributes
    String attrsSQL = PgAttribute.INSTANCE.getSQL(serverVersion);
    List<PgAttribute.Row> pgAttrs = queryResults(attrsSQL, PgAttribute.Row.class);

    //Load procs
    String procsSQL = PgProc.INSTANCE.getSQL(serverVersion);
    List<PgProc.Row> pgProcs = queryResults(procsSQL, PgProc.Row.class);

    logger.fine("query time: " + timer.getLap() + "ms");

    //Update the registry with known types
    registry.update(pgTypes, pgAttrs, pgProcs);

    logger.fine("load time: " + timer.getLap() + "ms");
  }

  private void prepareRefreshTypeQueries() throws IOException {

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
      //Load all new types we haven't seent
      refreshTypes(latestKnownTypeId);
    }

  }

  void refreshSpecificType(int typeId) {

    try {

      //Load types
      List<PgType.Row> pgTypes = queryResults("@refresh-type", PgType.Row.class, typeId);

      if (pgTypes.isEmpty()) {
        return;
      }

      //Load attributes
      List<PgAttribute.Row> pgAttrs = queryResults("@refresh-type-attrs", PgAttribute.Row.class, pgTypes.get(0).relationId);

      registry.update(pgTypes, pgAttrs, Collections.<PgProc.Row>emptyList());
    }
    catch (IOException | NoticeException e) {
      //Ignore errors
    }

  }

  void refreshTypes(int latestTypeId) {

    try {

      //Load types
      List<PgType.Row> pgTypes = queryResults("@refresh-types", PgType.Row.class, latestTypeId);

      if (pgTypes.isEmpty()) {
        return;
      }

      Integer[] typeIds = new Integer[pgTypes.size()];
      for (int c = 0; c < pgTypes.size(); ++c)
        typeIds[c] = pgTypes.get(c).relationId;

      //Load attributes
      List<PgAttribute.Row> pgAttrs = queryResults("@refresh-types-attrs", PgAttribute.Row.class, (Object) typeIds);

      registry.update(pgTypes, pgAttrs, Collections.<PgProc.Row>emptyList());
    }
    catch (IOException | NoticeException e) {
      logger.log(WARNING, "Error refreshing types", e);
    }

  }

  @Override
  public void refreshRelationType(int relationId) {

    try {

      //Load types
      List<PgType.Row> pgTypes = queryResults("@refresh-reltype", PgType.Row.class, relationId);

      if (pgTypes.isEmpty()) {
        return;
      }

      //Load attributes
      List<PgAttribute.Row> pgAttrs = queryResults("@refresh-type-attrs", PgAttribute.Row.class, relationId);

      registry.update(pgTypes, pgAttrs, Collections.<PgProc.Row>emptyList());
    }
    catch (IOException | NoticeException e) {
      //Ignore errors
    }

  }

  public boolean isUtilQueryPrepared(String name) {
    return utilQueries.containsKey(name);
  }

  public PreparedQuery prepareUtilQuery(String name, String sql, String... parameterTypeNames) throws IOException {

    List<Type> parameterTypes = new ArrayList<>(parameterTypeNames.length);
    for (String parameterTypeName : parameterTypeNames) {
      parameterTypes.add(registry.loadType(parameterTypeName));
    }

    return prepareUtilQuery(name, sql, parameterTypes);
  }

  public PreparedQuery prepareUtilQuery(String name, String sql, List<Type> parameterTypes) throws IOException {

    PrepareCommand prep = protocol.createPrepare(name, sql, parameterTypes);
    protocol.execute(prep);

    if (prep.getError() != null) {
      throw new IOException("unable to prepare query: " + prep.getError().getMessage());
    }

    PreparedQuery pq = new PreparedQuery(name, prep.getDescribedParameterTypes(), prep.getDescribedResultFields());
    utilQueries.put(name, pq);
    return pq;
  }

  private PreparedQuery prepareQuery(String queryTxt) throws NoticeException, IOException {

    if (queryTxt.charAt(0) == '@') {
      PreparedQuery util = utilQueries.get(queryTxt.substring(1));
      if (util == null) {
        throw new IOException("invalid utility query");
      }
      return util;
    }

    PrepareCommand prepare = protocol.createPrepare(null, queryTxt, Collections.<Type> emptyList());

    protocol.execute(prepare);

    if (prepare.getError() != null) {
      throw new NoticeException("Error preparing query", prepare.getError());
    }

    return new PreparedQuery(null, prepare.getDescribedParameterTypes(), prepare.getDescribedResultFields());
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> queryResults(String queryTxt, Class<T> rowType, Object... params) throws IOException, NoticeException {

    QueryCommand.ResultBatch resultBatch = queryBatch(queryTxt, rowType, params);

    return (List<T>) resultBatch.results;
  }

  @SuppressWarnings("unchecked")
  public List<Object> queryResults(String queryTxt) throws IOException, NoticeException {

    QueryCommand.ResultBatch resultBatch;

    if (queryTxt.charAt(0) == '@') {

      PreparedQuery pq = prepareQuery(queryTxt);

      resultBatch = preparedQuery(null, pq.name, Object[].class, Collections.<Type>emptyList(), Collections.emptyList(), pq.resultFields);
    }
    else {

      QueryCommand query = protocol.createQuery(queryTxt);

      protocol.execute(query);

      if (query.getError() != null) {
        throw new NoticeException("Error querying", query.getError());
      }

      List<QueryCommand.ResultBatch> resultBatches = query.getResultBatches();

      if (resultBatches.isEmpty()) {
        resultBatch = null;
      }
      else {
        resultBatch = query.getResultBatches().get(0);
      }

    }

    if (resultBatch == null) {
      return Collections.emptyList();
    }

    return (List<Object>) resultBatch.results;
  }

  public void query(String queryTxt) throws IOException, NoticeException {

    if (queryTxt.charAt(0) == '@') {

      PreparedQuery pq = prepareQuery(queryTxt);

      preparedQuery(null, pq.name, Object[].class, Collections.<Type> emptyList(), Collections.emptyList(), pq.resultFields);
    }

    QueryCommand query = protocol.createQuery(queryTxt);

    protocol.execute(query);

    if (query.getError() != null) {
      throw new NoticeException("Error querying", query.getError());
    }

  }

  public Object queryValue(String queryTxt) throws IOException, NoticeException {

    QueryCommand.ResultBatch resultBatch;

    if (queryTxt.charAt(0) == '@') {

      PreparedQuery pq = prepareQuery(queryTxt);

      resultBatch = preparedQuery(null, pq.name, Object[].class, Collections.<Type> emptyList(), Collections.emptyList(), pq.resultFields);
    }
    else {

      QueryCommand query = protocol.createQuery(queryTxt);

      protocol.execute(query);

      if (query.getError() != null) {
        throw new NoticeException("Error preparing query", query.getError());
      }

      List<QueryCommand.ResultBatch> res = query.getResultBatches();
      if (res.isEmpty()) {
        return null;
      }

      resultBatch = res.get(0);
    }

    if (resultBatch.results == null || resultBatch.results.isEmpty()) {
      return resultBatch.rowsAffected;
    }

    Object[] firstRow = (Object[]) resultBatch.results.get(0);
    if (firstRow.length == 0)
      return null;

    return firstRow[0];
  }

  public String queryFirstResultString(String queryTxt) throws IOException, NoticeException {

    List<Object> res = queryResults(queryTxt);
    if (res.isEmpty()) {
      return "";
    }

    Object[] firstRow = (Object[]) res.get(0);
    if (firstRow.length == 0)
      return "";

    if (firstRow[0] == null)
      return "";

    return firstRow[0].toString();
  }

  public QueryCommand.ResultBatch queryBatch(String queryTxt, Class<?> rowType, Object... params) throws IOException, NoticeException {

    PreparedQuery pq = prepareQuery(queryTxt);

    return preparedQuery(null, pq.name, rowType, pq.parameterTypes, asList(params), pq.resultFields);
  }

  private QueryCommand.ResultBatch preparedQuery(String portalName, String statementName, Class<?> rowType, List<Type> paramTypes, List<Object> paramValues,
      List<ResultField> resultFields) throws IOException, NoticeException {

    BindExecCommand query = protocol.createBindExec(portalName, statementName, paramTypes, paramValues, resultFields, rowType);

    protocol.execute(query);

    if (query.getError() != null) {
      throw new NoticeException("Error executing query", query.getError());
    }

    List<QueryCommand.ResultBatch> resultBatches = query.getResultBatches();
    if (resultBatches.isEmpty())
      return null;

    return resultBatches.get(0);
  }

  public void setKeyData(int processId, int secretKey) {

    keyData = new KeyData();
    keyData.processId = processId;
    keyData.secretKey = secretKey;
  }

  public void updateSystemParameter(String name, String value) {

    logger.config("system paramter: " + name + "=" + value);

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

  public void addNotificationListener(String name, String channelNameFilter, NotificationListener listener) {

    name = nullToEmpty(name);
    channelNameFilter = channelNameFilter != null ? channelNameFilter : ".*";

    Pattern channelNameFilterPattern = Pattern.compile(channelNameFilter);

    NotificationKey key = new NotificationKey(name, channelNameFilterPattern);

    synchronized (notificationListeners) {
      notificationListeners.put(key, new WeakReference<NotificationListener>(listener));
    }

  }

  public synchronized void removeNotificationListener(NotificationListener listener) {

    Iterator<Map.Entry<NotificationKey, WeakReference<NotificationListener>>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, WeakReference<NotificationListener>> entry = iter.next();

      NotificationListener iterListener = entry.getValue().get();
      if (iterListener == null || iterListener.equals(listener)) {

        iter.remove();
      }

    }
  }

  public synchronized void removeNotificationListener(String listenerName) {

    Iterator<Map.Entry<NotificationKey, WeakReference<NotificationListener>>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, WeakReference<NotificationListener>> entry = iter.next();

      String iterListenerName = entry.getKey().name;
      NotificationListener iterListener = entry.getValue().get();
      if (iterListenerName.equals(listenerName) || iterListener == null) {

        iter.remove();
      }

    }
  }

  @Override
  public synchronized void reportNotification(int processId, String channelName, String payload) {

    Iterator<Map.Entry<NotificationKey, WeakReference<NotificationListener>>> iter = notificationListeners.entrySet().iterator();
    while (iter.hasNext()) {

      Map.Entry<NotificationKey, WeakReference<NotificationListener>> entry = iter.next();

      NotificationListener listener = entry.getValue().get();
      if (listener == null) {

        iter.remove();
      }
      else if (entry.getKey().channelNameFilter.matcher(channelName).matches()) {

        listener.notification(processId, channelName, payload);
      }

    }

  }

}
