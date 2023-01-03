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
import com.impossibl.postgres.datetime.ISOIntervalFormat;
import com.impossibl.postgres.datetime.ISOTimeFormat;
import com.impossibl.postgres.datetime.ISOTimestampFormat;
import com.impossibl.postgres.datetime.IntervalFormat;
import com.impossibl.postgres.datetime.PostgresIntervalFormat;
import com.impossibl.postgres.protocol.FieldFormat;
import com.impossibl.postgres.protocol.FieldFormatRef;
import com.impossibl.postgres.protocol.RequestExecutor;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.ExecuteResult;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.PrepareResult;
import com.impossibl.postgres.protocol.RequestExecutorHandlers.QueryResult;
import com.impossibl.postgres.protocol.ResultBatch;
import com.impossibl.postgres.protocol.ResultField;
import com.impossibl.postgres.protocol.RowData;
import com.impossibl.postgres.protocol.ServerConnection;
import com.impossibl.postgres.protocol.ServerConnectionFactory;
import com.impossibl.postgres.system.tables.PGTypeTable;
import com.impossibl.postgres.types.ArrayType;
import com.impossibl.postgres.types.BaseType;
import com.impossibl.postgres.types.CompositeType;
import com.impossibl.postgres.types.DomainType;
import com.impossibl.postgres.types.EnumerationType;
import com.impossibl.postgres.types.MultiRangeType;
import com.impossibl.postgres.types.PsuedoType;
import com.impossibl.postgres.types.QualifiedName;
import com.impossibl.postgres.types.RangeType;
import com.impossibl.postgres.types.Registry;
import com.impossibl.postgres.types.SharedRegistry;
import com.impossibl.postgres.types.Type;
import com.impossibl.postgres.utils.ByteBufs;
import com.impossibl.postgres.utils.Locales;
import com.impossibl.postgres.utils.Timer;

import static com.impossibl.postgres.system.Empty.EMPTY_BUFFERS;
import static com.impossibl.postgres.system.Empty.EMPTY_FORMATS;
import static com.impossibl.postgres.system.Empty.EMPTY_TYPES;
import static com.impossibl.postgres.system.SystemSettings.APPLICATION_NAME;
import static com.impossibl.postgres.system.SystemSettings.DATABASE_NAME;
import static com.impossibl.postgres.system.SystemSettings.SESSION_USER;
import static com.impossibl.postgres.system.SystemSettings.STANDARD_CONFORMING_STRINGS;
import static com.impossibl.postgres.utils.guava.Strings.nullToEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelFuture;


public class BasicContext extends AbstractContext {

  private static final long INTERNAL_QUERY_TIMEOUT = SECONDS.toMillis(60);

  private static final Logger logger = Logger.getLogger(BasicContext.class.getName());

  private static class QueryDescription {

    String name;
    String sql;
    Type[] parameterTypes;
    ResultField[] resultFields;

    QueryDescription(String name, String sql, Type[] parameterTypes, ResultField[] resultFields) {
      this.name = name;
      this.sql = sql;
      this.parameterTypes = parameterTypes;
      this.resultFields = resultFields;
    }
  }

  private class ServerConnectionListener implements ServerConnection.Listener {

    @Override
    public void parameterStatusChanged(String name, String value) {
      updateSystemParameter(name, value);
    }

    @Override
    public void notificationReceived(int processId, String channelName, String payload) {
      connectionNotificationReceived(processId, channelName, payload);
    }

    @Override
    public InputStream openStandardInput() {
      return System.in;
    }

    @Override
    public OutputStream openStandardOutput() {
      return System.out;
    }

    @Override
    public void closed() {
      connectionClosed();
    }
  }

  private class RegistryTypeLoader implements Registry.TypeLoader {

    @Override
    public Type load(int oid) throws IOException {
      return BasicContext.this.loadType(oid);
    }

    @Override
    public CompositeType loadRelation(int relationOid) throws IOException {
      return BasicContext.this.loadRelationType(relationOid);
    }

    @Override
    public Type load(QualifiedName name) throws IOException {
      return BasicContext.this.loadType(name.toString());
    }

    @Override
    public Type load(String name) throws IOException {
      return BasicContext.this.loadType(name);
    }

  }


  protected Registry registry;
  protected Map<String, Class<?>> typeMap;
  protected Charset charset;
  protected Settings settings;
  private TimeZone timeZone;
  private ZoneId timeZoneId;
  private DateTimeFormat clientDateFormat;
  private DateTimeFormat serverDateFormat;
  private DateTimeFormat clientTimeFormat;
  private DateTimeFormat serverTimeFormat;
  private DateTimeFormat clientTimestampFormat;
  private DateTimeFormat serverTimestampFormat;
  private IntervalFormat clientIntervalFormat;
  private IntervalFormat serverIntervalFormat;
  private NumberFormat clientIntegerFormatter;
  private NumberFormat clientDecimalFormatter;
  private NumberFormat clientCurrencyFormatter;
  private NumberFormat serverCurrencyFormatter;
  private ServerConnection serverConnection;
  private ServerConnectionListener serverConnectionListener;
  private Map<String, QueryDescription> utilQueries;

  public BasicContext(SocketAddress address, Settings settings) throws IOException {
    this.typeMap = new HashMap<>();
    this.settings = settings;
    this.charset = UTF_8;
    this.timeZone = TimeZone.getTimeZone("UTC");
    this.clientDateFormat = new ISODateFormat();
    this.serverDateFormat = clientDateFormat;
    this.clientTimeFormat = new ISOTimeFormat();
    this.serverTimeFormat = clientTimeFormat;
    this.clientTimestampFormat = new ISOTimestampFormat();
    this.serverTimestampFormat = clientTimestampFormat;
    this.clientIntervalFormat = new ISOIntervalFormat();
    this.serverIntervalFormat = clientIntervalFormat;
    this.serverConnectionListener = new ServerConnectionListener();
    this.serverConnection = ServerConnectionFactory.getDefault().connect(this, address, serverConnectionListener);
    this.utilQueries = new HashMap<>();
  }

  protected ChannelFuture shutdown() {

    return serverConnection.shutdown();
  }

  /**
   * Called when {@link #serverConnection} was closed
   * externally (i.e. without calling {@link #shutdown()}
   */
  protected void connectionClosed() {
    shutdown().awaitUninterruptibly();
  }

  /**
   * Called when {@link #serverConnection} received
   * an asynchronous notification
   */
  protected void connectionNotificationReceived(int processId, String channelName, String payload) {
  }

  public ByteBufAllocator getAllocator() {
    return serverConnection.getAllocator();
  }

  protected ServerConnection getServerConnection() {
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
  public <T> T getSetting(Setting<T> setting) {
    T value = settings.getStored(setting);
    if (value != null)
      return value;
    return super.getSetting(setting);
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
  public ZoneId getTimeZoneId() {
    return timeZoneId;
  }

  @Override
  public ServerInfo getServerInfo() {
    return serverConnection.getServerInfo();
  }

  @Override
  public ServerConnection.KeyData getKeyData() {
    return serverConnection.getKeyData();
  }

  @Override
  public DateTimeFormat getServerDateFormat() {
    return serverDateFormat;
  }

  @Override
  public DateTimeFormat getClientDateFormat() {
    return clientDateFormat;
  }

  @Override
  public DateTimeFormat getServerTimeFormat() {
    return serverTimeFormat;
  }

  @Override
  public DateTimeFormat getClientTimeFormat() {
    return clientTimeFormat;
  }

  @Override
  public IntervalFormat getServerIntervalFormat() {
    return serverIntervalFormat;
  }

  @Override
  public IntervalFormat getClientIntervalFormat() {
    return clientIntervalFormat;
  }

  @Override
  public DateTimeFormat getServerTimestampFormat() {
    return serverTimestampFormat;
  }

  @Override
  public DateTimeFormat getClientTimestampFormat() {
    return clientTimestampFormat;
  }

  @Override
  public NumberFormat getClientIntegerFormatter() {
    return clientIntegerFormatter;
  }

  @Override
  public NumberFormat getClientDecimalFormatter() {
    return clientDecimalFormatter;
  }

  @Override
  public NumberFormat getServerCurrencyFormatter() {
    return serverCurrencyFormatter;
  }

  @Override
  public NumberFormat getClientCurrencyFormatter() {
    return clientCurrencyFormatter;
  }

  protected void init(SharedRegistry.Factory sharedRegistryFactory) throws IOException {

    String database = getSetting(DATABASE_NAME, getSetting(SESSION_USER));

    ServerConnectionInfo serverConnectionInfo =
        new ServerConnectionInfo(serverConnection.getServerInfo(), serverConnection.getRemoteAddress(), database);

    registry = new Registry(sharedRegistryFactory.get(serverConnectionInfo), new RegistryTypeLoader());

    clientIntegerFormatter = NumberFormat.getIntegerInstance(Locale.getDefault());
    clientIntegerFormatter.setGroupingUsed(false);
    clientIntegerFormatter.setParseIntegerOnly(true);

    clientDecimalFormatter = DecimalFormat.getNumberInstance(Locale.getDefault());
    clientDecimalFormatter.setGroupingUsed(false);
    ((DecimalFormat)clientDecimalFormatter).setParseBigDecimal(true);

    serverCurrencyFormatter = DecimalFormat.getCurrencyInstance(Locale.ROOT);
    serverCurrencyFormatter.setGroupingUsed(false);
    ((DecimalFormat)serverCurrencyFormatter).setParseBigDecimal(true);

    clientCurrencyFormatter = DecimalFormat.getCurrencyInstance(Locale.getDefault());
    clientCurrencyFormatter.setGroupingUsed(false);
    ((DecimalFormat)clientCurrencyFormatter).setParseBigDecimal(true);

    loadTypes();

    prepareRefreshTypeQueries();

    loadServerLocales();
  }

  private void loadServerLocales() throws IOException {

    try (ResultBatch resultBatch =
        queryBatch("SELECT name, setting FROM pg_settings WHERE name IN ('lc_monetary')", INTERNAL_QUERY_TIMEOUT)) {

      for (RowData rowData : resultBatch.borrowRows().borrowAll()) {

        String localeSpec = rowData.getField(1, resultBatch.getFields()[1], this, String.class, null).toString();

        Locale locale = Locales.parseLocale(localeSpec);
        if (locale == null) {
          // Default to ROOT locale with appropriate warning
          logger.log(Level.WARNING, "Locale {} could not be mapped to a Java locale, using the default (aka POSIX) locale", localeSpec);
          locale = Locale.ROOT;
        }

        String name = rowData.getField(0, resultBatch.getFields()[1], this, String.class, null).toString();
        if ("lc_monetary".equals(name)) {
          serverCurrencyFormatter = NumberFormat.getCurrencyInstance(locale);
          serverCurrencyFormatter.setGroupingUsed(false);
          ((DecimalFormat)serverCurrencyFormatter).setParseBigDecimal(true);
        }

      }
    }

  }

  private void loadTypes() throws IOException {

    SharedRegistry.Seeder seeder = registry -> {

      logger.config("Seeding registry");

      Timer timer = new Timer();

      // Load "simple" types only - composite types are loaded on demand
      String typeSQL = PGTypeTable.INSTANCE.getSQL(serverConnection.getServerInfo().getVersion());
      List<PGTypeTable.Row> pgTypes = PGTypeTable.INSTANCE.query(this, typeSQL + " WHERE typrelid = 0", INTERNAL_QUERY_TIMEOUT);

      // Load initial types without causing refresh queries...
      //

      // First, base types...
      Set<PGTypeTable.Row> baseTypeRows = pgTypes.stream()
          .filter(PGTypeTable.Row::isBase)
          .collect(toSet());
      Set<Integer> baseTypeOids = baseTypeRows.stream()
          .map(PGTypeTable.Row::getOid)
          .collect(toSet());
      Set<PGTypeTable.Row> baseReferencingRows = pgTypes.stream()
          .filter(row -> baseTypeOids.contains(row.getReferencingTypeOid()))
          .collect(toSet());

      List<Type> baseTypes = new ArrayList<>();
      for (PGTypeTable.Row row : baseTypeRows) {
        if (!row.isArray()) {
          Type type = loadRaw(row);
          baseTypes.add(type);
        }
      }
      registry.addTypes(baseTypes);

      // Now, types that reference base types (arrays, ranges, domains, etc)

      List<Type> baseReferencingTypes = new ArrayList<>();
      for (PGTypeTable.Row baseReferencingRow : baseReferencingRows) {
        Type type = loadRaw(baseReferencingRow);
        baseReferencingTypes.add(type);
      }
      registry.addTypes(baseReferencingTypes);

      // Next, psuedo types
      List<Type> psuedoTypes = new ArrayList<>();
      for (PGTypeTable.Row pgType : pgTypes) {
        if (pgType.isPsuedo() && !registry.hasTypeDefined(pgType.getOid())) {
          Type type = loadRaw(pgType);
          psuedoTypes.add(type);
        }
      }
      registry.addTypes(psuedoTypes);

      logger.fine("Seed time: " + timer.getLap() + "ms");

    };

    if (!registry.getShared().seed(seeder)) {
      logger.config("Using pre-seeded registry");
    }
  }

  private void prepareRefreshTypeQueries() throws IOException {

    Version serverVersion = serverConnection.getServerInfo().getVersion();

    prepareUtilQuery("refresh-type", PGTypeTable.INSTANCE.getSQL(serverVersion) + " WHERE t.oid = $1");

    prepareUtilQuery("refresh-named-type", PGTypeTable.INSTANCE.getSQL(serverVersion) + " WHERE t.oid = $1::text::regtype");

    prepareUtilQuery("refresh-reltype", PGTypeTable.INSTANCE.getSQL(serverVersion) + " WHERE t.typrelid = $1", "int4");

  }

  private Type loadType(int typeId) throws IOException {

    //Load types
    List<PGTypeTable.Row> pgTypes = PGTypeTable.INSTANCE.query(this, "@refresh-type", INTERNAL_QUERY_TIMEOUT, typeId);
    if (pgTypes.isEmpty()) {
      return null;
    }

    PGTypeTable.Row pgType  = pgTypes.get(0);

    return loadRaw(pgType);
  }

  private Type loadType(String typeName) throws IOException {

    //Load types
    List<PGTypeTable.Row> pgTypes = PGTypeTable.INSTANCE.query(this, "@refresh-named-type", INTERNAL_QUERY_TIMEOUT, typeName);
    if (pgTypes.isEmpty()) {
      return null;
    }

    PGTypeTable.Row pgType  = pgTypes.get(0);

    return loadRaw(pgType);
  }

  private CompositeType loadRelationType(int relationId) throws IOException {

    //Load types
    List<PGTypeTable.Row> pgTypes = PGTypeTable.INSTANCE.query(this, "@refresh-reltype", INTERNAL_QUERY_TIMEOUT, relationId);
    if (pgTypes.isEmpty()) {
      return null;
    }

    PGTypeTable.Row pgType = pgTypes.get(0);
    if (pgType.getRelationId() == 0) {
      return null;
    }

    return (CompositeType) loadRaw(pgType);
  }

  /*
   * Materialize a type from the given "pg_type" and "pg_attribute" data
   */
  private Type loadRaw(PGTypeTable.Row pgType) throws IOException {

    Type type;

    if (pgType.getElementTypeId() != 0 && pgType.getCategory().equals("A")) {

      type = new ArrayType();
    }
    else {

      switch (pgType.getDiscriminator().charAt(0)) {
        case 'b':
          type = new BaseType();
          break;
        case 'c':
          type = new CompositeType();
          break;
        case 'd':
          type = new DomainType();
          break;
        case 'e':
          type = new EnumerationType();
          break;
        case 'p':
          type = new PsuedoType();
          break;
        case 'r':
          type = new RangeType();
          break;
        case 'm':
          type = new MultiRangeType();
          break;
        default:
          logger.warning("unknown discriminator (aka 'typtype') found in pg_type table");
          return null;
      }

    }

    type.load(pgType, registry);

    return type;
  }

  public boolean isUtilQueryPrepared(String name) {
    return utilQueries.containsKey(name);
  }

  public void prepareUtilQuery(String name, String sql, String... parameterTypeNames) throws IOException {

    Type[] parameterTypes = new Type[parameterTypeNames.length];
    for (int parameterIdx = 0; parameterIdx < parameterTypes.length; ++parameterIdx) {
      parameterTypes[parameterIdx] = registry.loadBaseType(parameterTypeNames[parameterIdx]);
    }

    prepareUtilQuery(name, sql, parameterTypes);
  }

  private void prepareUtilQuery(String name, String sql, Type[] parameterTypes) throws IOException {

    PrepareResult handler = new PrepareResult();

    serverConnection.getRequestExecutor().prepare(name, sql, parameterTypes, handler);

    handler.await(INTERNAL_QUERY_TIMEOUT, MILLISECONDS);

    QueryDescription desc = new QueryDescription(name, sql, handler.getDescribedParameterTypes(this), handler.getDescribedResultFields());
    utilQueries.put(name, desc);
  }

  private QueryDescription prepareQuery(String queryTxt) throws IOException {

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

    return new QueryDescription(null, queryTxt, handler.getDescribedParameterTypes(this), handler.getDescribedResultFields());
  }

  public void query(String queryTxt, long timeout) throws IOException {

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

  protected String queryString(String queryTxt, long timeout) throws IOException {

    try (ResultBatch resultBatch = queryBatch(queryTxt, timeout)) {
      Object field = resultBatch.borrowRows().borrow(0)
          .getField(0, resultBatch.getFields()[0], this, String.class, null);
      String val = field == null ? null : field.toString();
      return nullToEmpty(val);
    }

  }

  /**
   * Queries for a single (the first) result batch. The batch must be released.
   */
  protected ResultBatch queryBatch(String queryTxt, long timeout) throws IOException {

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
  public ResultBatch queryBatchPrepared(String queryTxt, Object[] paramValues, long timeout) throws IOException {

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
                                           long timeout) throws IOException {

    QueryDescription pq = prepareQuery(queryTxt);

    return queryBatchPrepared(pq.name, paramFormats, paramBuffers, pq.resultFields, timeout);
  }

  /**
   * Queries a single result batch (the first) via a parameterized query. The batch must be released.
   */
  private ResultBatch queryBatchPrepared(String statementName,
                                         FieldFormatRef[] paramFormats, ByteBuf[] paramBuffers,
                                         ResultField[] resultFields, long timeout) throws IOException {

    ExecuteResult handler = new ExecuteResult(resultFields);

    serverConnection.getRequestExecutor()
        .execute(null, statementName, paramFormats, paramBuffers, resultFields, 0, handler);

    handler.await(timeout, MILLISECONDS);

    return handler.getBatch();
  }

  private void updateSystemParameter(String name, String value) {

    logger.config("system parameter: " + name + "=" + value);

    switch (name) {

      case ParameterNames.DATE_STYLE:

        String[] parsedDateStyle = DateStyle.parse(value);

        if (parsedDateStyle == null) {
          logger.warning("Invalid DateStyle encountered");
        }
        else {

          serverDateFormat = DateStyle.getDateFormat(parsedDateStyle);
          if (serverDateFormat == null) {
            logger.warning("Unknown Date format, reverting to default");
            serverDateFormat = new ISODateFormat();
          }

          serverTimeFormat = DateStyle.getTimeFormat(parsedDateStyle);
          if (serverTimeFormat == null) {
            logger.warning("Unknown Time format, reverting to default");
            serverTimeFormat = new ISOTimeFormat();
          }

          serverTimestampFormat = DateStyle.getTimestampFormat(parsedDateStyle);
          if (serverTimestampFormat == null) {
            logger.warning("Unknown Timestamp format, reverting to default");
            serverTimestampFormat = new ISOTimestampFormat();
          }
        }
        break;

      case ParameterNames.INTERVAL_STYLE:

        try {
          IntervalStyle intervalStyle = IntervalStyle.valueOf(value.toUpperCase());

          switch (intervalStyle) {
            case ISO_8601:
              serverIntervalFormat = new ISOIntervalFormat();
              break;

            case POSTGRES:
            case POSTGRES_VERBOSE:
              serverIntervalFormat = new PostgresIntervalFormat();
              break;

            case SQL_STANDARD:
              logger.warning("Unsupported IntervalStyle, reverting to default");
              serverIntervalFormat = new PostgresIntervalFormat();
              break;
          }

        }
        catch (IllegalArgumentException e) {
          logger.warning("Unrecognized IntervalStyle encountered");
        }
        break;

      case ParameterNames.TIME_ZONE:
        if (value.contains("+")) {
          value = value.replace('+', '-');
        }
        else {
          value = value.replace('-', '+');
        }

        timeZone = TimeZone.getTimeZone(value);
        timeZoneId = timeZone.toZoneId();
        break;

      case ParameterNames.CLIENT_ENCODING:

        charset = Charset.forName(value);
        break;

      case ParameterNames.STANDARD_CONFORMING_STRINGS:

        settings.set(STANDARD_CONFORMING_STRINGS, value.equals("on"));
        break;

      case ParameterNames.SESSION_AUTHORIZATION:

        settings.set(SESSION_USER, value);
        break;

      case ParameterNames.APPLICATION_NAME:

        settings.set(APPLICATION_NAME, value);
        break;

      default:
        break;
    }

  }

  @Override
  public Context unwrap() {
    return this;
  }

}
