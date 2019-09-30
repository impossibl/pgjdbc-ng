/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2018 Jesper Pedersen <jesper.pedersen@comcast.net>
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.impossibl.postgres.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.LineNumberReader;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;



/**
 * Replay
 *
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class Replay {

  /** Default configuration */
  private static final String DEFAULT_CONFIGURATION = "replay.properties";

  /** Log line type: EOF */
  private static final int EOF = -1;

  /** Log line type: UNKNOWN */
  private static final int UNKNOWN = 0;

  /** Log line type: PANIC */
  private static final int PANIC = 1;

  /** Log line type: FATAL */
  private static final int FATAL = 2;

  /** Log line type: ERROR */
  private static final int ERROR = 3;

  /** Log line type: WARNING */
  private static final int WARNING = 4;

  /** Log line type: INFO */
  private static final int INFO = 5;

  /** Log line type: DEBUG1 */
  private static final int DEBUG1 = 6;

  /** Log line type: DEBUG2 */
  private static final int DEBUG2 = 7;

  /** Log line type: DEBUG3 */
  private static final int DEBUG3 = 8;

  /** Log line type: DEBUG4 */
  private static final int DEBUG4 = 9;

  /** Log line type: DEBUG5 */
  private static final int DEBUG5 = 10;

  /** Log line type: STATEMENT */
  private static final int STATEMENT = 11;

  /** Log line type: DETAIL */
  private static final int DETAIL = 12;

  /** Log line type: LOG */
  private static final int LOG = 13;

  /** Log line type: NOTICE */
  private static final int NOTICE = 14;

  /** Log line type: HINT */
  private static final int HINT = 15;

  /** The configuration */
  private static Properties configuration;

  /** Data:          Process  LogEntry */
  private static Map<Integer, List<LogEntry>> data = new TreeMap<>();

  /** The file name */
  private static String filename;

  /** The profile name */
  private static String profilename;

  /** Column types   Table       Column  Type */
  private static Map<String, Map<String, Integer>> columnTypes = new TreeMap<>();

  /** Aliases:       Alias   Name */
  private static Map<String, String> aliases = new TreeMap<>();

  /** Current table name */
  private static String currentTableName = null;

  /** Iterate through ResultSet */
  private static boolean resultSet = false;

  /** Parallel execution */
  private static boolean parallelExecution = true;

  /** XA */
  private static boolean xa = false;

  /** Error */
  private static boolean error = false;

  /** Wait */
  private static boolean wait = false;

  /** XADataSource */
  private static XADataSource xaDataSource = null;

  /** NG driver */
  private static boolean ngDriver = false;

  /**
   * Write data to a file
   *
   * @param p The path of the file
   * @param l The data
   */
  private static void writeFile(Path p, List<String> l) throws Exception {
    BufferedWriter bw = Files.newBufferedWriter(p,
                                                StandardOpenOption.CREATE,
                                                StandardOpenOption.WRITE,
                                                StandardOpenOption.TRUNCATE_EXISTING);
    for (String s : l) {
      bw.write(s, 0, s.length());
      bw.newLine();
    }

    bw.flush();
    bw.close();
  }

  /**
   * Get the type of the log line
   *
   * @param s The string
   *
   * @return The type
   */
  private static int getLogLineType(String s) {
    if (s == null || "".equals(s)) {
      return EOF;
    }

    int bracket1Start = s.indexOf("[");
    int bracket1End = s.indexOf("]");

    if (bracket1Start != -1) {
      int bracket2Start = s.indexOf("[", bracket1End + 1);
      int bracket2End = s.indexOf("]", bracket1End + 1);

      String type = s.substring(bracket2End + 2, s.indexOf(":", bracket2End + 2));

      if ("LOG".equals(type)) {
        return LOG;
      }
      else if ("STATEMENT".equals(type)) {
        return STATEMENT;
      }
      else if ("DETAIL".equals(type)) {
        return DETAIL;
      }
      else if ("NOTICE".equals(type)) {
        return NOTICE;
      }
      else if ("PANIC".equals(type)) {
        return PANIC;
      }
      else if ("FATAL".equals(type)) {
        return FATAL;
      }
      else if ("ERROR".equals(type)) {
        return ERROR;
      }
      else if ("WARNING".equals(type)) {
        return WARNING;
      }
      else if ("INFO".equals(type)) {
        return INFO;
      }
      else if ("DEBUG1".equals(type)) {
        return DEBUG1;
      }
      else if ("DEBUG2".equals(type)) {
        return DEBUG2;
      }
      else if ("DEBUG3".equals(type)) {
        return DEBUG3;
      }
      else if ("DEBUG4".equals(type)) {
        return DEBUG4;
      }
      else if ("DEBUG5".equals(type)) {
        return DEBUG5;
      }
      else if ("HINT".equals(type)) {
        return HINT;
      }
      else {
        System.out.println("Unknown log line type for: " + s);
        System.exit(1);
      }
    }

    return UNKNOWN;
  }

  /**
   * Process the log
   */
  private static void processLog() throws Exception {
    FileReader fr = null;
    LineNumberReader lnr = null;
    String s = null;
    String str = null;
    LogEntry le = null;
    boolean execute = false;
    try {
      fr = new FileReader(Paths.get(filename).toFile());
      lnr = new LineNumberReader(fr);
      s = lnr.readLine();

      while (s != null) {
        str = s;
        s = lnr.readLine();

        if (s == null || "".equals(s)) {
          return;
        }

        while (getLogLineType(s) == UNKNOWN) {
          str += " ";
          str += s.trim();
          s = lnr.readLine();
        }

        le = new LogEntry(s);

        if (le.isParse() || le.isBind()) {
          execute = false;
        }
        else if (le.isExecute() || (execute && le.isParameters())) {
          execute = true;

          List<LogEntry> lle = data.get(le.getProcessId());
          if (lle == null) {
            lle = new ArrayList<>();
          }
          lle.add(le);
          data.put(le.getProcessId(), lle);
        }
        else if (le.isStmt()) {
          execute = false;

          List<LogEntry> lle = data.get(le.getProcessId());
          if (lle == null) {
            lle = new ArrayList<>();
          }
          lle.add(le);
          data.put(le.getProcessId(), lle);
        }
      }
    }
    catch (Exception e) {
      System.out.println("Line: " + (lnr != null ? lnr.getLineNumber() : "?"));
      System.out.println("Data:");
      System.out.println(str);
      System.out.println("Line:");
      System.out.println(s);
      throw e;
    }
    finally {
      if (lnr != null) {
        lnr.close();
      }

      if (fr != null) {
        fr.close();
      }
    }
  }

  /**
   * Create interaction
   *
   * @param c The connection
   */
  private static void createInteraction(Connection c) throws Exception {
    for (Integer proc : data.keySet()) {
      List<LogEntry> lle = data.get(proc);
      List<String> l = new ArrayList<>();

      for (int i = 0; i < lle.size(); i++) {
        LogEntry le = lle.get(i);
        if (le.isExecute() || le.isStmt()) {
          DataEntry de = new DataEntry();
          de.setPrepared(le.isPrepared());

          String stmt = le.getStatement();
          if (stmt.startsWith("PREPARE TRANSACTION")) {
            continue;
          }
          else if (stmt.startsWith("COMMIT PREPARED")) {
            stmt = "COMMIT";
          }
          else if (stmt.startsWith("ROLLBACK PREPARED")) {
            stmt = "ROLLBACK";
          }
          de.setStatement(stmt);

          if (i < lle.size() - 1) {
            LogEntry next = lle.get(i + 1);
            if (next.isParameters()) {
              de.setPrepared(true);

              List<String> parameters = new ArrayList<>();
              StringTokenizer st = new StringTokenizer(next.getStatement(), "?");
              List<Integer> types = getParameterTypes(c, le.getStatement(), st.countTokens());

              while (st.hasMoreTokens()) {
                String token = st.nextToken();
                int start = token.indexOf("'");
                int end = token.lastIndexOf("'");
                String value = null;
                if (start != -1 && end != -1) {
                  if (start + 1 == end) {
                    value = "";
                  }
                  else {
                    value = token.substring(start + 1, end);
                  }
                }
                parameters.add(value);
              }

              de.setTypes(types);
              de.setParameters(parameters);
              i++;
            }
          }

          l.addAll(de.getData());
        }
      }

      if (l.size() > 0) {
        writeFile(Paths.get(profilename, proc + ".cli"), l);
      }
    }
  }

  /**
   * Get the parameter types of a query
   *
   * @param c The connection
   * @param query The query
   * @param num The number of required parameter types
   *
   * @return The types
   */
  private static List<Integer> getParameterTypes(Connection c, String query, int num) throws Exception {
    List<Integer> result = new ArrayList<>();

    try {
      net.sf.jsqlparser.statement.Statement s = CCJSqlParserUtil.parse(query);

      if (s instanceof Select) {
        Select select = (Select) s;

        StringBuilder buffer = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
          private Column currentColumn = null;

          @Override
          public void visit(Column column) {
            currentColumn = column;
          }

          @Override
          public void visit(JdbcParameter jdbcParameter) {
            String table = null;
            String column = currentColumn.getColumnName();

            if (currentColumn.getTable() != null) {
              table = currentColumn.getTable().getName();
            }
            else {
              table = currentTableName;
            }

            if (aliases.containsKey(table)) {
              table = aliases.get(table);
            }

            try {
              result.add(getType(c, table, column));
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        };

        SelectDeParser deparser = new SelectDeParser(expressionDeParser, buffer) {
          @Override
          public void visit(Table table) {
            currentTableName = table.getName();

            if (table.getAlias() != null && !table.getAlias().getName().equals("")) {
              aliases.put(table.getAlias().getName(), table.getName());
            }
          }
        };
        expressionDeParser.setSelectVisitor(deparser);
        expressionDeParser.setBuffer(buffer);

        select.getSelectBody().accept(deparser);

        if (select.getSelectBody() instanceof PlainSelect) {
          PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
          if (plainSelect.getLimit() != null) {
            result.add(Integer.valueOf(Types.INTEGER));
          }
        }
      }
      else if (s instanceof Update) {
        Update update = (Update) s;

        for (Table table : update.getTables()) {
          currentTableName = table.getName();
        }

        StringBuilder buffer = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
          private Column currentColumn = null;

          @Override
          public void visit(Column column) {
            currentColumn = column;
          }

          @Override
          public void visit(JdbcParameter jdbcParameter) {
            String table = null;
            String column = currentColumn.getColumnName();

            if (currentColumn.getTable() != null) {
              table = currentColumn.getTable().getName();
            }
            else {
              table = currentTableName;
            }

            if (aliases.containsKey(table)) {
              table = aliases.get(table);
            }

            try {
              result.add(getType(c, table, column));
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        };

        SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer) {
          @Override
          public void visit(Table table) {
            currentTableName = table.getName();

            if (table.getAlias() != null && !table.getAlias().getName().equals("")) {
              aliases.put(table.getAlias().getName(), table.getName());
            }
          }
        };
        expressionDeParser.setSelectVisitor(selectDeParser);
        expressionDeParser.setBuffer(buffer);

        net.sf.jsqlparser.util.deparser.UpdateDeParser updateDeParser =
            new net.sf.jsqlparser.util.deparser.UpdateDeParser(expressionDeParser, selectDeParser, buffer);

        net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
            new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer) {
              @Override
              public void visit(Update update) {
                updateDeParser.deParse(update);
              }
            };

        update.accept(statementDeParser);
      }
      else if (s instanceof Delete) {
        Delete delete = (Delete) s;
        currentTableName = delete.getTable().getName();

        StringBuilder buffer = new StringBuilder();
        ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
          private Column currentColumn = null;

          @Override
          public void visit(Column column) {
            currentColumn = column;
          }

          @Override
          public void visit(JdbcParameter jdbcParameter) {
            String table = null;
            String column = currentColumn.getColumnName();

            if (currentColumn.getTable() != null) {
              table = currentColumn.getTable().getName();
            }
            else {
              table = currentTableName;
            }

            if (aliases.containsKey(table)) {
              table = aliases.get(table);
            }

            try {
              result.add(getType(c, table, column));
            }
            catch (Exception e) {
              e.printStackTrace();
            }
          }
        };
        expressionDeParser.setBuffer(buffer);

        net.sf.jsqlparser.util.deparser.DeleteDeParser deleteDeParser =
            new net.sf.jsqlparser.util.deparser.DeleteDeParser(expressionDeParser, buffer);

        net.sf.jsqlparser.util.deparser.StatementDeParser statementDeParser =
            new net.sf.jsqlparser.util.deparser.StatementDeParser(buffer) {
              @Override
              public void visit(Delete delete) {
                deleteDeParser.deParse(delete);
              }
            };

        delete.accept(statementDeParser);
      }
      else if (s instanceof Insert) {
        Insert insert = (Insert) s;
        currentTableName = insert.getTable().getName();

        for (Column currentColumn : insert.getColumns()) {
          String table = null;
          String column = currentColumn.getColumnName();

          if (currentColumn.getTable() != null) {
            table = currentColumn.getTable().getName();
          }
          else {
            table = currentTableName;
          }

          if (aliases.containsKey(table)) {
            table = aliases.get(table);
          }

          try {
            result.add(getType(c, table, column));
          }
          catch (Exception e) {
            e.printStackTrace();
          }
        }
      }

      if (result.size() != num) {
        System.out.println("Incomplete data for query: " + query);
        System.out.println(result);
        System.out.println(num);
        System.out.println("-------");
      }

      return result;
    }
    catch (Exception e) {
      System.out.println("Exception with: " + query + " (" + num + ")");
      throw e;
    }
  }

  /**
   * Get the type of a column
   *
   * @param c The connection
   * @param table The table
   * @param column The column
   *
   * @return The type
   */
  private static int getType(Connection c, String table, String column) throws Exception {
    Map<String, Integer> tableData = columnTypes.get(table.toLowerCase(Locale.US));

    if (tableData == null) {
      tableData = new TreeMap<>();
      ResultSet rs = null;
      try {
        DatabaseMetaData dmd = c.getMetaData();
        rs = dmd.getColumns(null, null, table.toLowerCase(Locale.US), "");
        while (rs.next()) {
          String columnName = rs.getString("COLUMN_NAME");
          int dataType = rs.getInt("DATA_TYPE");
          tableData.put(columnName.toLowerCase(Locale.US), dataType);
        }

        columnTypes.put(table.toLowerCase(Locale.US), tableData);
      }
      finally {
        if (rs != null) {
          try {
            rs.close();
          }
          catch (Exception e) {
            // Ignore
          }
        }
      }
    }

    return tableData.get(column.toLowerCase(Locale.US));
  }

  /**
   * Execute clients
   */
  private static void executeClients() throws Exception {
    File directory = new File(profilename);
    File[] clientData = directory.listFiles((dir, filename) -> filename.endsWith(".cli"));
    Arrays.sort(clientData);

    List<Client> clients = new ArrayList<>(clientData.length);
    CountDownLatch clientReady = new CountDownLatch(clientData.length);
    CountDownLatch clientRun = new CountDownLatch(1);
    CountDownLatch clientDone = new CountDownLatch(clientData.length);
    int statements = 0;
    ExecutorService es = null;
    long start = 0;
    long end = 0;
    boolean quiet = Boolean.valueOf(configuration.getProperty("quiet", "false"));

    if (!quiet) {
      System.out.print("Recreating... ");
    }

    recreate();

    if (!quiet) {
      System.out.println("Done");
      System.out.print("Analyzing... ");
    }

    analyze();

    if (!quiet) {
      System.out.println("Done");
      System.out.print("Preparing... ");
    }

    for (File f : clientData) {
      List<String> l = Files.readAllLines(f.toPath());
      List<DataEntry> interaction = new ArrayList<>();

      int i = 0;
      while (l.get(i).startsWith("#")) {
        i++;
      }

      while (i < l.size()) {
        String prepared = l.get(i++);
        String statement = l.get(i++);
        String types = l.get(i++);
        String parameters = l.get(i++);
        interaction.add(new DataEntry(prepared, statement, types, parameters));
        statements++;
      }

      clients.add(new Client(Integer.valueOf(f.getName().substring(0, f.getName().indexOf("."))),
                             interaction,
                             clientReady, clientRun, clientDone));
    }

    if (parallelExecution) {
      String mc = configuration.getProperty("max_connections");
      if (mc == null) {
        es = Executors.newFixedThreadPool(clients.size(), r -> new Thread(r, "Replay Client"));

        for (Client cli : clients) {
          es.submit(cli);
        }

        clientReady.await();

        if (wait) {
          System.out.println("Press Enter to Start");
          System.console().readLine();
        }

        start = System.currentTimeMillis();
        clientRun.countDown();

        if (!quiet) {
          System.out.println("Done");
        }

        clientDone.await();
        end = System.currentTimeMillis();
      }
      else {
        int maxConnections = Integer.valueOf(mc);
        es = Executors.newFixedThreadPool(maxConnections);

        for (Client cli : clients) {
          es.submit(cli);
        }

        if (clients.size() <= maxConnections) {
          clientReady.await();
        }
        else {
          Thread.sleep(2000L);
        }

        if (wait) {
          System.out.println("Press Enter to Start");
          System.console().readLine();
        }

        start = System.currentTimeMillis();
        clientRun.countDown();

        if (!quiet) {
          System.out.println("Done");
        }

        clientDone.await();
        end = System.currentTimeMillis();
      }
    }
    else {
      clientRun.countDown();

      if (!quiet) {
        System.out.println("Done");
      }

      start = System.currentTimeMillis();
      for (Client cli : clients) {
        cli.run();
        if (!cli.isSuccess() && !error) {
          break;
        }
      }
      end = System.currentTimeMillis();
    }

    DecimalFormat secsFmt = new DecimalFormat("0.00");

    System.out.println("Clock: " + secsFmt.format(((double)(end - start)/1000.f)) + "ms");
    System.out.println("  Number of clients: " + clients.size());
    System.out.println("  Statements: " + statements);
    for (Client cli : clients) {
      StringBuilder sb = new StringBuilder();
      sb = sb.append("  ");
      sb = sb.append(cli.getId());
      sb = sb.append(": ");
      sb = sb.append(cli.getRunTime());
      sb = sb.append("/");
      sb = sb.append(cli.getConnectionTime());
      sb = sb.append("/");
      sb = sb.append(cli.getStatements());
      sb = sb.append("/");
      sb = sb.append(cli.skipped);
      if (error) {
        sb = sb.append("/");
        sb = sb.append(cli.getErrors());
      }

      System.out.println(sb.toString());
    }

    if (es != null) {
      es.shutdown();
    }

    writeCSV(end - start, clients);
  }

  /**
   * Write CSV file
   *
   * @param clock The clock time
   * @param clients The clients
   */
  private static void writeCSV(long clock, List<Client> clients) throws Exception {
    List<String> l = new ArrayList<>();

    l.add("Clock," + clock + "," + clock);

    for (Client cli : clients) {
      StringBuilder sb = new StringBuilder();
      sb = sb.append(cli.getId());
      sb = sb.append(",");
      sb = sb.append(cli.getRunTime());
      sb = sb.append(",");
      sb = sb.append(cli.getConnectionTime());
      sb = sb.append(",");
      sb = sb.append(cli.getStatements());
      sb = sb.append(",");
      sb = sb.append(cli.skipped);
      if (error) {
        sb = sb.append(",");
        sb = sb.append(cli.getErrors());
      }
      l.add(sb.toString());
    }

    writeFile(Paths.get(profilename, "result.csv"), l);
  }

  /**
   * Read the configuration (replay.properties)
   *
   * @param config The configuration
   */
  private static void readConfiguration(String config) throws Exception {
    File f = new File(config);
    configuration = new Properties();

    if (f.exists()) {
      FileInputStream fis = null;
      try {
        fis = new FileInputStream(f);
        configuration.load(fis);
      }
      finally {
        if (fis != null) {
          try {
            fis.close();
          }
          catch (Exception e) {
            // Nothing todo
          }
        }
      }
    }
  }

  /**
   * Get a XAConnection
   *
   * @return The connection
   */
  private static synchronized XAConnection getXAConnection() throws Exception {
    if (xaDataSource == null) {
      String host = configuration.getProperty("host", "localhost");
      int port = Integer.valueOf(configuration.getProperty("port", "5432"));
      String database = configuration.getProperty("database");

      if (!ngDriver) {
        Class<?> clz = Class.forName("org.postgresql.xa.PGXADataSource");
        Object obj = clz.newInstance();

        Method mHost = clz.getMethod("setServerName", String.class);
        mHost.invoke(obj, host);

        Method mPort = clz.getMethod("setPortNumber", int.class);
        mPort.invoke(obj, port);

        Method mDatabase = clz.getMethod("setDatabaseName", String.class);
        mDatabase.invoke(obj, database);

        xaDataSource = (XADataSource) obj;
      }
      else {
        Class<?> clz = Class.forName("com.impossibl.postgres.jdbc.xa.PGXADataSource");
        Object obj = clz.newInstance();

        Method mHost = clz.getMethod("setHost", String.class);
        mHost.invoke(obj, host);

        Method mPort = clz.getMethod("setPort", int.class);
        mPort.invoke(obj, port);

        Method mDatabase = clz.getMethod("setDatabase", String.class);
        mDatabase.invoke(obj, database);

        xaDataSource = (XADataSource) obj;
      }
    }

    return xaDataSource.getXAConnection(configuration.getProperty("user"),
                                        configuration.getProperty("password"));
  }

  /**
   * ANALYZE
   */
  private static void recreate() throws Exception {
    Connection c = null;
    Statement stmt = null;
    try {
      String url = null;
      if (!ngDriver) {
        url = "jdbc:postgresql://" + configuration.getProperty("host", "localhost") + ":" +
              configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database") + "_src";
      }
      else {
        url = "jdbc:pgsql://" + configuration.getProperty("host", "localhost") + ":" +
              configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database") + "_src";
      }

      String query = configuration.getProperty("query");
      if (!query.isEmpty()) {
        url += "?" + query;
      }

      c = DriverManager.getConnection(url);

      stmt = c.createStatement();
      stmt.executeUpdate("DROP DATABASE IF EXISTS " + configuration.getProperty("database"));
      stmt.executeUpdate("CREATE DATABASE " +
          configuration.getProperty("database") + " WITH TEMPLATE " +
          configuration.getProperty("database") + "_src OWNER " +
          configuration.getProperty("user") + ";"
      );
    }
    finally {
      if (stmt != null) {
        try {
          stmt.close();
          stmt = null;
        }
        catch (SQLException se) {
          // Ignore
        }
      }
      if (c != null) {
        try {
          c.close();
          c = null;
        }
        catch (SQLException se) {
          // Ignore
        }
      }
    }
  }

  /**
   * ANALYZE
   */
  private static void analyze() throws Exception {
    Connection c = null;
    Statement stmt = null;
    try {
      String url = null;
      if (!ngDriver) {
        url = "jdbc:postgresql://" + configuration.getProperty("host", "localhost") + ":" +
              configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database");
      }
      else {
        url = "jdbc:pgsql://" + configuration.getProperty("host", "localhost") + ":" +
              configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database");
      }

      String query = configuration.getProperty("query");
      if (!query.isEmpty()) {
        url += "?" + query;
      }

      c = DriverManager.getConnection(url);

      stmt = c.createStatement();
      stmt.execute("ANALYZE");
    }
    finally {
      if (stmt != null) {
        try {
          stmt.close();
        }
        catch (SQLException se) {
          // Ignore
        }
      }
      if (c != null) {
        try {
          c.close();
        }
        catch (SQLException se) {
          // Ignore
        }
      }
    }
  }

  /**
   * Usage
   */
  private static void usage() {
    System.out.println("Usage: Replay -i <log_file>                      (init)");
    System.out.println("       Replay [-r] [-s] [-x] [-e] [-w] <profile> (run)");
  }

  /**
   * Main
   *
   * @param args The arguments
   */
  public static void main(String[] args) {
    Connection c = null;
    try {
      if (args.length == 0) {
        usage();
        return;
      }

      String config = DEFAULT_CONFIGURATION;
      readConfiguration(config);

      String url = null;

      if (configuration.getProperty("url") == null) {
        String database = configuration.getProperty("database");
        if (database == null) {
          System.out.println("database not defined.");
          return;
        }

        url = "jdbc:postgresql://" + configuration.getProperty("host", "localhost") +
              ":" + configuration.getProperty("port", "5432") + "/" + database;
      }
      else {
        url = configuration.getProperty("url");

        int doubleSlash = url.indexOf("//");
        int slash = url.indexOf("/", doubleSlash + 2);
        int col = url.indexOf(":", doubleSlash);
        int query = url.indexOf('?', slash);
        String host = url.substring(doubleSlash + 2, col != -1 ? col : slash);
        String port = "5432";
        String database = url.substring(slash + 1, query != -1 ? query : url.length());
        String queryStr = query != -1 ? url.substring(query + 1) : "";

        if (col != -1) {
          port = url.substring(col + 1, slash);
        }

        if (url.contains(":pgsql:")) {
          ngDriver = true;
          System.out.println(">> NG DRIVER <<");
        }
        else {
          System.out.println(">> OG DRIVER <<");
        }

        configuration.setProperty("host", host);
        configuration.setProperty("port", port);
        configuration.setProperty("database", database);
        configuration.setProperty("query", queryStr);
      }

      String user = configuration.getProperty("user");
      if (user == null) {
        System.out.println("user not defined.");
        return;
      }

      String password = configuration.getProperty("password");
      if (password == null) {
        System.out.println("password not defined.");
        return;
      }

      if ("-i".equals(args[0])) {
        if (args.length == 1) {
          usage();
          return;
        }

        filename = args[1];

        profilename = filename.substring(0, filename.lastIndexOf("."));

        c = DriverManager.getConnection(url);

        File directory = new File(profilename);
        if (directory.exists()) {
          Files.walk(Paths.get(profilename))
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        }
        directory.mkdirs();

        processLog();
        createInteraction(c);
      }
      else {
        for (int parameter = 0; parameter < args.length - 1; parameter++) {
          if ("-r".equals(args[parameter])) {
            resultSet = true;
          }
          else if ("-s".equals(args[parameter])) {
            parallelExecution = false;
          }
          else if ("-x".equals(args[parameter])) {
            xa = true;
          }
          else if ("-e".equals(args[parameter])) {
            error = true;
          }
          else if ("-w".equals(args[parameter])) {
            wait = true;
          }
        }

        profilename = args[args.length - 1];

        executeClients();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      if (c != null) {
        try {
          c.close();
        }
        catch (Exception e) {
          // Nothing to do
        }
      }
    }
  }

  /**
   * Client
   */
  static class Client implements Runnable {

    /** Identifier */
    private int identifier;

    /** Interaction data */
    private List<DataEntry> interaction;

    /** Client ready */
    private CountDownLatch clientReady;

    /** Client run */
    private CountDownLatch clientRun;

    /** Client done */
    private CountDownLatch clientDone;

    /** Success */
    private boolean success;

    /** Before connection */
    private long beforeConnection;

    /** After connection */
    private long afterConnection;

    /** Before run */
    private long beforeRun;

    /** After run */
    private long afterRun;

    /** Errors */
    private int errors;

    private int skipped;

    /**
     * Constructor
     */
    Client(int identifier, List<DataEntry> interaction,
           CountDownLatch clientReady, CountDownLatch clientRun, CountDownLatch clientDone) {
      this.identifier = identifier;
      this.interaction = interaction;
      this.clientReady = clientReady;
      this.clientRun = clientRun;
      this.clientDone = clientDone;
      this.success = false;
      this.beforeConnection = 0;
      this.afterConnection = 0;
      this.beforeRun = 0;
      this.afterRun = 0;
      this.errors = 0;
    }

    /**
     * Get id
     *
     * @return The value
     */
    int getId() {
      return identifier;
    }

    /**
     * Is success
     *
     * @return The value
     */
    boolean isSuccess() {
      return success;
    }

    /**
     * Get the connection time
     *
     * @return The value
     */
    long getConnectionTime() {
      return afterConnection - beforeConnection;
    }

    /**
     * Get the run time
     *
     * @return The value
     */
    long getRunTime() {
      return afterRun - beforeRun;
    }

    /**
     * Get number of statements
     *
     * @return The value
     */
    int getStatements() {
      return interaction.size();
    }

    /**
     * Get errors
     *
     * @return The value
     */
    int getErrors() {
      return errors;
    }

    /**
     * Do the interaction
     */
    public void run() {
      beforeConnection = System.currentTimeMillis();
      XAConnection xc = null;
      Xid xid = null;
      Connection c = null;
      DataEntry de = null;
      try {
        try {
          if (xa) {
            xc = getXAConnection();
            c = xc.getConnection();
            xid = new XidImpl(identifier);
          }
          else {
            String url;
            if (!ngDriver) {
              url = "jdbc:postgresql://" + configuration.getProperty("host", "localhost") + ":" +
                    configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database");
            }
            else {
              url = "jdbc:pgsql://" + configuration.getProperty("host", "localhost") + ":" +
                    configuration.getProperty("port", "5432") + "/" + configuration.getProperty("database");
            }

            String query = configuration.getProperty("query");
            if (!query.isEmpty()) {
              url += "?" + query;
            }

            c = DriverManager.getConnection(url);
          }
          afterConnection = System.currentTimeMillis();
        }
        catch (Exception ce) {
          clientReady.countDown();
          throw ce;
        }

        clientReady.countDown();
        clientRun.await();
        beforeRun = System.currentTimeMillis();

        for (int counter = 0; counter < interaction.size(); counter++) {
          de = interaction.get(counter);
          if ("BEGIN".equals(de.getStatement())) {
            c.setAutoCommit(false);
            if (xa) {
              xc.getXAResource().start(xid, XAResource.TMNOFLAGS);
            }
          }
          else if ("ROLLBACK".equals(de.getStatement())) {
            if (xa) {
              xc.getXAResource().end(xid, XAResource.TMFAIL);
              xc.getXAResource().prepare(xid);
              xc.getXAResource().rollback(xid);
            }
            else {
              c.rollback();
            }
            c.setAutoCommit(true);
          }
          else if ("COMMIT".equals(de.getStatement())) {
            if (xa) {
              xc.getXAResource().end(xid, XAResource.TMSUCCESS);
              xc.getXAResource().prepare(xid);
              xc.getXAResource().commit(xid, false);
            }
            else {
              c.commit();
            }
            c.setAutoCommit(true);
          }
          else {
            if (de.getStatement().startsWith("SELECT")) {
              //                     ++skipped;
              //                     continue;
            }

            if (!de.isPrepared()) {
              Statement stmt = c.createStatement();
              try {
                if (stmt.execute(de.getStatement()) && resultSet) {
                  ResultSet rs = stmt.getResultSet();
                  while (rs.next()) {
                    // Just advance
                  }
                  rs.close();
                }
              }
              catch (Exception sqle) {
                if (error) {
                  errors++;
                }
                else {
                  throw sqle;
                }
              }
              finally {
                stmt.close();
              }
            }
            else {
              PreparedStatement ps = c.prepareStatement(de.getStatement());

              List<Integer> types = de.getTypes();
              List<String> parameters = de.getParameters();

              if (types != null) {
                for (int i = 0; i < types.size(); i++) {
                  int type = types.get(i);
                  String value = parameters.get(i);

                  if ("null".equals(value)) {
                    ps.setObject(i + 1, null);
                  }
                  else {
                    switch (type) {
                      case Types.BINARY:
                        ps.setBytes(i + 1, Utils.hexDecode(value.substring(2)));
                        break;
                      case Types.BIT:
                        ps.setBoolean(i + 1, Boolean.valueOf(value));
                        break;
                      case Types.BIGINT:
                        ps.setLong(i + 1, Long.valueOf(value));
                        break;
                      case Types.BOOLEAN:
                        ps.setBoolean(i + 1, Boolean.valueOf(value));
                        break;
                      case Types.CHAR:
                        ps.setString(i + 1, value);
                        break;
                      case Types.DATE:
                        ps.setDate(i + 1, java.sql.Date.valueOf(value));
                        break;
                      case Types.DECIMAL:
                        ps.setDouble(i + 1, Double.valueOf(value));
                        break;
                      case Types.DOUBLE:
                        ps.setDouble(i + 1, Double.valueOf(value));
                        break;
                      case Types.FLOAT:
                        ps.setFloat(i + 1, Float.valueOf(value));
                        break;
                      case Types.INTEGER:
                        ps.setInt(i + 1, Integer.valueOf(value));
                        break;
                      case Types.LONGVARBINARY:
                        ps.setBytes(i + 1, Utils.hexDecode(value.substring(2)));
                        break;
                      case Types.LONGVARCHAR:
                        ps.setString(i + 1, value);
                        break;
                      case Types.NUMERIC:
                        ps.setDouble(i + 1, Double.valueOf(value));
                        break;
                      case Types.REAL:
                        ps.setFloat(i + 1, Float.valueOf(value));
                        break;
                      case Types.SMALLINT:
                        ps.setShort(i + 1, Short.valueOf(value));
                        break;
                      case Types.TIME:
                      case Types.TIME_WITH_TIMEZONE:
                        ps.setTime(i + 1, java.sql.Time.valueOf(value));
                        break;
                      case Types.TIMESTAMP:
                      case Types.TIMESTAMP_WITH_TIMEZONE:
                        ps.setTimestamp(i + 1, java.sql.Timestamp.valueOf(value));
                        break;
                      case Types.TINYINT:
                        ps.setShort(i + 1, Short.valueOf(value));
                        break;
                      case Types.VARBINARY:
                        ps.setBytes(i + 1, Utils.hexDecode(value.substring(2)));
                        break;
                      case Types.VARCHAR:
                        ps.setString(i + 1, value);
                        break;
                      default:
                        System.out.println("Unsupported value: " + type);
                        break;
                    }
                  }
                }
              }

              try {
                if (ps.execute() && resultSet) {
                  ResultSet rs = ps.getResultSet();
                  while (rs.next()) {
                    // Just advance
                  }
                  rs.close();
                }
              }
              catch (Exception sqle) {
                if (error) {
                  errors++;
                }
                else {
                  throw sqle;
                }
              }
              finally {
                ps.close();
              }
            }
          }
        }

        afterRun = System.currentTimeMillis();
        success = true;
      }
      catch (Exception e) {
        beforeRun = 0;
        afterRun = 0;

        System.out.println("Exception from client " + identifier);
        System.out.println(de);
        e.printStackTrace();
      }
      finally {
        if (c != null) {
          try {
            c.close();
          }
          catch (Exception e) {
            // Nothing to do
          }
        }
        clientDone.countDown();
      }
    }
  }



  /**
   * Log entry
   */
  static class LogEntry {

    private int processId;
    private String timestamp;
    private int transactionId;
    private String fullStatement;
    private String statement;
    private boolean prepared;
    private boolean parse;
    private boolean bind;
    private boolean execute;
    private boolean parameters;
    private boolean stmt;

    LogEntry(String s) {
      int bracket1Start = s.indexOf("[");
      int bracket1End = s.indexOf("]");
      int bracket2Start = s.indexOf("[", bracket1End + 1);
      int bracket2End = s.indexOf("]", bracket1End + 1);

      this.processId = Integer.valueOf(s.substring(0, bracket1Start).trim());
      this.timestamp = s.substring(bracket1Start + 1, bracket1End);
      this.transactionId = Integer.valueOf(s.substring(bracket2Start + 1, bracket2End));
      this.fullStatement = s.substring(bracket2End + 2);

      this.statement = null;
      this.prepared = false;

      this.parse = isParse(this.fullStatement);
      this.bind = false;
      this.execute = false;
      this.parameters = false;
      this.stmt = false;

      if (!parse) {
        this.bind = isBind(this.fullStatement);
        if (!bind) {
          this.execute = isExecute(this.fullStatement);
          if (!execute) {
            this.parameters = isParameters(this.fullStatement);
            if (!parameters) {
              this.stmt = isStmt(this.fullStatement);
            }
          }
        }
      }
    }

    int getProcessId() {
      return processId;
    }

    String getTimestamp() {
      return timestamp;
    }

    int getTransactionId() {
      return transactionId;
    }

    String getStatement() {
      return statement;
    }

    boolean isPrepared() {
      return prepared;
    }

    boolean isParse() {
      return parse;
    }

    boolean isBind() {
      return bind;
    }

    boolean isExecute() {
      return execute;
    }

    boolean isParameters() {
      return parameters;
    }

    boolean isStmt() {
      return stmt;
    }

    /**
     * Is parse
     *
     * @param line The log line
     *
     * @return True if parse, otherwise false
     */
    private boolean isParse(String line) {
      int offset = line.indexOf("parse ");
      if (offset != -1) {
        statement = line.substring(line.indexOf(":", offset) + 2);
        statement = statement.replaceAll("\\$[0-9]*", "?");
        prepared = line.indexOf("<unnamed>") != -1;
        return true;
      }

      return false;
    }

    /**
     * Is bind
     *
     * @param line The log line
     *
     * @return True if bind, otherwise false
     */
    private boolean isBind(String line) {
      int offset = line.indexOf("bind ");
      if (offset != -1) {
        statement = line.substring(line.indexOf(":", offset) + 2);
        statement = statement.replaceAll("\\$[0-9]*", "?");
        prepared = line.indexOf("<unnamed>") != -1;
        return true;
      }

      return false;
    }

    /**
     * Is execute
     *
     * @param line The log line
     *
     * @return True if execute, otherwise false
     */
    private boolean isExecute(String line) {
      int offset = line.indexOf("execute ");
      if (offset != -1) {
        statement = line.substring(line.indexOf(":", offset) + 2);
        statement = statement.replaceAll("\\$[0-9]*", "?");
        prepared = line.indexOf("<unnamed>") != -1;
        return true;
      }

      return false;
    }

    /**
     * Is parameters
     *
     * @param line The log line
     *
     * @return True if execute, otherwise false
     */
    private boolean isParameters(String line) {
      int offset = line.indexOf("DETAIL:  parameters:");
      if (offset != -1) {
        statement = line.substring(offset + 21);
        statement = statement.replaceAll("\\$[0-9]*", "?");
        return true;
      }

      return false;
    }

    /**
     * Is stmt
     *
     * @param line The log line
     *
     * @return True if stmt, otherwise false
     */
    private boolean isStmt(String line) {
      int offset = line.indexOf("statement:");
      if (offset != -1) {
        statement = line.substring(line.indexOf(":", offset) + 2);
        statement = statement.replaceAll("\\$[0-9]*", "?");
        prepared = false;
        return true;
      }

      return false;
    }

    @Override
    public String toString() {
      return processId + " [" + timestamp + "] [" + transactionId + "] " + fullStatement;
    }
  }



  /**
   * Data entry
   */
  static class DataEntry {

    private boolean prepared;
    private String statement;
    private List<Integer> types;
    private List<String> parameters;

    DataEntry() {
      prepared = false;
      statement = null;
      types = null;
      parameters = null;
    }

    DataEntry(String p, String s, String t, String pa) {
      this();
      try {
        prepared = "P".equals(p);
        statement = s;
        if (t != null && !"".equals(t)) {
          types = new ArrayList<>();
          String[] ss = t.split("\\|");
          for (int i = 0; i < ss.length; i++) {
            types.add(Integer.valueOf(ss[i]));
          }
        }
        if (pa != null && !"".equals(pa)) {
          parameters = new ArrayList<>();
          String[] ss = pa.split("\\|");
          for (int i = 0; i < ss.length; i++) {
            parameters.add(ss[i]);
          }
        }
      }
      catch (Exception e) {
        System.out.println("P: " + p);
        System.out.println("S: " + s);
        System.out.println("T: " + t);
        System.out.println("PA: " + pa);
        throw e;
      }
    }

    /**
     * Is prepared
     *
     * @return The value
     */
    boolean isPrepared() {
      return prepared;
    }

    /**
     * Set prepared
     *
     * @param v The value
     */
    void setPrepared(boolean v) {
      prepared = v;
    }

    /**
     * Get statement
     *
     * @return The value
     */
    String getStatement() {
      return statement;
    }

    /**
     * Set statement
     *
     * @param v The value
     */
    void setStatement(String v) {
      statement = v;
    }

    /**
     * Get types
     *
     * @return The values
     */
    List<Integer> getTypes() {
      return types;
    }

    /**
     * Set types
     *
     * @param v The value
     */
    void setTypes(List<Integer> v) {
      types = v;
    }

    /**
     * Get parameters
     *
     * @return The values
     */
    List<String> getParameters() {
      return parameters;
    }

    /**
     * Set parameters
     *
     * @param v The value
     */
    void setParameters(List<String> v) {
      parameters = v;
    }

    /**
     * Get data
     *
     * @return The data
     */
    List<String> getData() {
      List<String> result = new ArrayList<>();

      result.add(prepared ? "P" : "S");
      result.add(statement);

      if (types != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < types.size(); i++) {
          sb = sb.append(types.get(i));
          if (i < types.size() - 1) {
            sb = sb.append("|");
          }
        }
        result.add(sb.toString());
      }
      else {
        result.add("");
      }

      if (parameters != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parameters.size(); i++) {
          sb = sb.append(parameters.get(i));
          if (i < parameters.size() - 1) {
            sb = sb.append("|");
          }
        }
        result.add(sb.toString());
      }
      else {
        result.add("");
      }

      return result;
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
      return getData().toString();
    }
  }



  /**
   * Basic Xid implementation
   */
  static class XidImpl implements Xid {

    private int id;

    /**
     * Constructor
     *
     * @param id The identifier
     */
    public XidImpl(int id) {
      this.id = id;
    }

    /**
     * {@inheritDoc}
     */
    public int getFormatId() {
      return id;
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getGlobalTransactionId() {
      return new byte[]{ (byte) (id >>> 24), (byte) (id >>> 16), (byte) (id >>> 8), (byte) id };
    }

    /**
     * {@inheritDoc}
     */
    public byte[] getBranchQualifier() {
      return new byte[]{ (byte) (id >>> 24), (byte) (id >>> 16), (byte) (id >>> 8), (byte) id };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }

      if (other == null || !(other instanceof XidImpl)) {
        return false;
      }

      XidImpl x = (XidImpl) other;

      return id == x.id;
    }
  }
}



class Utils {

  public static byte[] hexDecode(String hex) {
    int l = hex.length();
    byte[] data = new byte[l / 2];
    for (int i = 0; i < l; i += 2) {
      data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                            + Character.digit(hex.charAt(i + 1), 16));
    }
    return data;
  }
}
