/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 Jesper Pedersen <jesper.pedersen@comcast.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

/**
 * Generate a SQL workload for Replay
 * @author <a href="jesper.pedersen@comcast.net">Jesper Pedersen</a>
 */
public class SQLLoadGenerator
{
   /** Default profile */
   private static final String DEFAULT_PROFILE = "sqlloadgenerator";

   /** Default number of rows */
   private static final int DEFAULT_ROWS = 1000;

   /** Default number of clients */
   private static final int DEFAULT_CLIENTS = 10;

   /** Default number of statements */
   private static final int DEFAULT_STATEMENTS = 10000;

   /** Default max number of statements per transaction */
   private static final int DEFAULT_MAX_STATEMENTS_PER_TRANSACTION = 5;

   /** Default SELECT mix */
   private static final int DEFAULT_MIX_SELECT = 70;

   /** Default UPDATE mix */
   private static final int DEFAULT_MIX_UPDATE = 15;

   /** Default INSERT mix */
   private static final int DEFAULT_MIX_INSERT = 10;

   /** Default DELETE mix */
   private static final int DEFAULT_MIX_DELETE = 5;

   /** Default COMMIT */
   private static final int DEFAULT_COMMIT = 100;

   /** Default ROLLBACK */
   private static final int DEFAULT_ROLLBACK = 0;

   /** Default scale */
   private static final double DEFAULT_SCALE = 1.0;

   /** Default NOT NULL */
   private static final int DEFAULT_NOT_NULL = 100;

   /** Profile */
   private static Properties profile;

   /** Scale */
   private static double scale = 1.0;

   /** Random */
   private static Random random = new Random();

   /** Column names   Table   Names */
   private static Map<String, List<String>> columnNames = new HashMap<>();
   
   /** Column types   Table   Types */
   private static Map<String, List<String>> columnTypes = new HashMap<>();
   
   /** Primary keys   Table   Column */
   private static Map<String, String> primaryKeys = new HashMap<>();

   /** Active PKs     Table       Client           PKs */
   private static Map<String, Map<Integer, List<String>>> activePKs = new HashMap<>();

   /** Foreign keys   Table           Column */
   private static Map<String, Set<String>> foreignKeys = new HashMap<>();

   /** FromTo         Table           Table */
   private static Map<String, Set<String>> fromTo = new HashMap<>();

   /** ToFrom         Table           Table */
   private static Map<String, Set<String>> toFrom = new HashMap<>();

   /** NOT NULL       Table           Column */
   private static Map<String, Set<String>> notNulls = new HashMap<>();

   /** UNIQUE         Table       Column          Value */
   private static Map<String, Map<String, Set<String>>> uniques = new HashMap<>();

   /**
    * Write data to a file
    * @param p The path of the file
    * @param l The data
    */
   private static void writeFile(Path p, List<String> l) throws Exception
   {
      BufferedWriter bw = Files.newBufferedWriter(p,
                                                  StandardOpenOption.CREATE,
                                                  StandardOpenOption.WRITE,
                                                  StandardOpenOption.TRUNCATE_EXISTING);
      for (String s : l)
      {
         bw.write(s, 0, s.length());
         bw.newLine();
      }

      bw.flush();
      bw.close();
   }

   /**
    * Write ddl.sql
    * @param profileName The name of the profile
    */
   private static void writeDDL(String profileName) throws Exception
   {
      List<String> l = new ArrayList<>();
      Enumeration<?> e = profile.propertyNames();
      List<String> alter = new ArrayList<>();

      // Tables
      while (e.hasMoreElements())
      {
         String key = (String)e.nextElement();
         if (key.startsWith("table."))
         {
            String tableName = key.substring(key.indexOf(".") + 1);
            String description = profile.getProperty(key);
            String primaryKey = null;
            int counter = 1;

            List<String> colNames = new ArrayList<>();
            List<String> colTypes = new ArrayList<>();
            List<String> colDescriptions = new ArrayList<>();
            
            while (profile.getProperty(tableName + ".column." + counter) != null)
            {
               String name = profile.getProperty(tableName + ".column." + counter);
               String type = profile.getProperty(tableName + ".column." + counter + ".type");
               String desc = profile.getProperty(tableName + ".column." + counter + ".description");
               String pk = profile.getProperty(tableName + ".column." + counter + ".primarykey");
               String fkTable = profile.getProperty(tableName + ".column." + counter + ".foreignkey.table");
               String fkColumn = profile.getProperty(tableName + ".column." + counter + ".foreignkey.column");
               String notnull = profile.getProperty(tableName + ".column." + counter + ".notnull");
               String unique = profile.getProperty(tableName + ".column." + counter + ".unique");

               if (type == null)
                  type = "int";

               verifyType(type);

               if (desc != null && !"".equals(desc.trim()))
               {
                  StringBuilder sb = new StringBuilder();
                  sb.append("COMMENT ON COLUMN ");
                  sb.append(tableName);
                  sb.append(".");
                  sb.append(name);
                  sb.append(" IS \'");
                  sb.append(desc.trim());
                  sb.append("\';");
                  colDescriptions.add(sb.toString());
               }
               
               if (pk != null && !"".equals(pk.trim()))
               {
                  if (Boolean.parseBoolean(pk))
                  {
                     if (primaryKey == null)
                     {
                        primaryKey = name;
                        primaryKeys.put(tableName, primaryKey);
                     }
                     else
                     {
                        throw new Exception("Already have primary key \'" + primaryKey + "\' on table " + tableName);
                     }
                  }
               }

               if (fkTable != null && fkColumn != null)
               {
                  Set<String> ts = foreignKeys.get(tableName);

                  if (ts == null)
                     ts = new HashSet<>();

                  ts.add(name + ":" + fkTable + ":" + fkColumn);
                  foreignKeys.put(tableName, ts);

                  ts = fromTo.get(tableName);

                  if (ts == null)
                     ts = new HashSet<>();

                  ts.add(fkTable);
                  fromTo.put(tableName, ts);

                  ts = toFrom.get(fkTable);

                  if (ts == null)
                     ts = new HashSet<>();

                  ts.add(tableName);
                  toFrom.put(fkTable, ts);

                  StringBuilder sb = new StringBuilder();
                  sb.append("ALTER TABLE ONLY ");
                  sb.append(tableName);
                  sb.append(" ADD CONSTRAINT ");
                  sb.append("fk_");
                  sb.append(tableName);
                  sb.append("_");
                  sb.append(name);
                  sb.append("_");
                  sb.append(fkTable);
                  sb.append("_");
                  sb.append(fkColumn);
                  sb.append(" FOREIGN KEY (");
                  sb.append(name);
                  sb.append(") REFERENCES ");
                  sb.append(fkTable);
                  sb.append("(");
                  sb.append(fkColumn);
                  sb.append(");");

                  alter.add(sb.toString());
               }

               if (notnull != null && !"".equals(notnull.trim()))
               {
                  if (Boolean.parseBoolean(notnull))
                  {
                     Set<String> ts = notNulls.get(tableName);

                     if (ts == null)
                        ts = new HashSet<>();

                     ts.add(name);
                     notNulls.put(tableName, ts);
                  }
               }

               if (unique != null && !"".equals(unique.trim()))
               {
                  if (Boolean.parseBoolean(unique))
                  {
                     Map<String, Set<String>> m = uniques.get(tableName);

                     if (m == null)
                        m = new HashMap<>();

                     Set<String> ts = m.get(name);

                     if (ts == null)
                        ts = new HashSet<>();

                     m.put(name, ts);
                     uniques.put(tableName, m);

                     StringBuilder sb = new StringBuilder();
                     sb.append("ALTER TABLE ONLY ");
                     sb.append(tableName);
                     sb.append(" ADD CONSTRAINT ");
                     sb.append("uniq_");
                     sb.append(tableName);
                     sb.append("_");
                     sb.append(name);
                     sb.append(" UNIQUE (");
                     sb.append(name);
                     sb.append(");");

                     alter.add(sb.toString());
                  }
               }

               colNames.add(name);
               colTypes.add(type);

               counter++;
            }

            if (colNames.size() > 0)
            {
               columnNames.put(tableName, colNames);
               columnTypes.put(tableName, colTypes);
            
               StringBuilder sb = new StringBuilder();
               sb.append("CREATE TABLE ");
               sb.append(tableName);
               sb.append(" (");
               for (int i = 0; i < colNames.size(); i++)
               {
                  sb.append(colNames.get(i));
                  sb.append(" ");
                  sb.append(colTypes.get(i));
                  if (primaryKey != null && primaryKey.equals(colNames.get(i)))
                  {
                     sb.append(" PRIMARY KEY");
                  }
                  else if (!isNullable(tableName, colNames.get(i)))
                  {
                     sb.append(" NOT NULL");
                  }
                  if (i < colNames.size() - 1)
                     sb.append(", ");
               }
               sb.append(");");
            
               l.add(sb.toString());

               if (description != null && !"".equals(description.trim()))
               {
                  sb = new StringBuilder();
                  sb.append("COMMENT ON TABLE ");
                  sb.append(tableName);
                  sb.append(" IS \'");
                  sb.append(description.trim());
                  sb.append("\';");
                  l.add(sb.toString());
               }

               l.addAll(colDescriptions);

               if (primaryKey == null)
               {
                  sb = new StringBuilder();
                  sb.append("CREATE INDEX idx_");
                  sb.append(tableName);
                  sb.append("_");
                  sb.append(colNames.get(0));
                  sb.append(" ON ");
                  sb.append(tableName);
                  if (!isBTreeIndex(colTypes.get(0)))
                  {
                     sb.append(" USING HASH");
                  }
                  sb.append(" (");
                  sb.append(colNames.get(0));
                  sb.append(");");

                  l.add(sb.toString());
               }
            }
            else
            {
               System.out.println("No columns for " + tableName);
            }
         }
      }
      
      if (alter.size() > 0)
         l.addAll(alter);

      e = profile.propertyNames();

      // Indexes
      while (e.hasMoreElements())
      {
         String key = (String)e.nextElement();
         if (key.startsWith("index."))
         {
            String tableName = key.substring(key.indexOf(".") + 1, key.lastIndexOf("."));
            String cols = profile.getProperty(key);

            String colNames = cols;
            colNames = colNames.replace(" ", "_");
            colNames = colNames.replace(",", "");

            boolean hash = false;
            if (cols.indexOf(",") == -1)
            {
               List<String> names = columnNames.get(tableName);
               List<String> types = columnTypes.get(tableName);
               int index = names.indexOf(cols);

               if (index != -1 && !isBTreeIndex(types.get(index)))
                  hash = true;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("CREATE INDEX IF NOT EXISTS idx_");
            sb.append(tableName);
            sb.append("_");
            sb.append(colNames);
            sb.append(" ON ");
            sb.append(tableName);
            if (hash)
            {
               sb.append(" USING HASH");
            }
            sb.append(" (");
            sb.append(cols);
            sb.append(");");
            
            l.add(sb.toString());
         }
      }
      
      writeFile(Paths.get(profileName, "ddl.sql"), l);
   }

   /**
    * Write data.sql
    * @param profileName The name of the profile
    */
   private static void writeData(String profileName) throws Exception
   {
      List<String> l = new ArrayList<>();

      // No dependencies
      for (String tableName : columnNames.keySet())
      {
         if (!fromTo.containsKey(tableName))
         {
            l.addAll(getINSERT(tableName));
            l.add("");
         }
      }

      // Dependencies
      for (String tableName : columnNames.keySet())
      {
         if (fromTo.containsKey(tableName))
         {
            l.addAll(getINSERT(tableName));
            l.add("");
         }
      }
      
      l.add("ANALYZE;");
      writeFile(Paths.get(profileName, "data.sql"), l);
   }

   /**
    * Get INSERTs for a table
    * @param tableName The name of the table
    * @return The INSERT data
    */
   private static List<String> getINSERT(String tableName) throws Exception
   {
      int rows = getRows(tableName);
      List<String> colNames = columnNames.get(tableName);
      List<String> colTypes = columnTypes.get(tableName);

      List<String> l = new ArrayList<>(rows);

      for (int row = 1; row <= rows; row++)
      {
         StringBuilder sb = new StringBuilder();
         sb.append("INSERT INTO ");
         sb.append(tableName);
         sb.append(" (");
         for (int i = 0; i < colNames.size(); i++)
         {
            sb.append(colNames.get(i));
            if (i < colNames.size() - 1)
               sb.append(", ");
         }
         sb.append(") VALUES (");
         for (int i = 0; i < colTypes.size(); i++)
         {
            String val;
            if (isPrimaryKey(tableName, colNames.get(i)))
            {
               val = generatePrimaryKey(0, tableName, colNames.get(i), colTypes.get(i), row, false);
            }
            else if (isForeignKey(tableName, colNames.get(i)))
            {
               val = generateForeignKey(0, tableName, colNames.get(i));
            }
            else if (isUnique(tableName, colNames.get(i)))
            {
               val = generateUnique(tableName, colNames.get(i), colTypes.get(i));
            }
            else
            {
               val = getData(tableName, colNames.get(i), colTypes.get(i), row, i == 0 ? false : true);
            }

            if (!"NULL".equals(val) && mustEscape(colTypes.get(i)))
               sb.append("\'");
            if (isPrimaryKey(tableName, colNames.get(i)))
            {
               sb.append(val);
            }
            else if (isForeignKey(tableName, colNames.get(i)))
            {
               sb.append(val);
            }
            else if (isUnique(tableName, colNames.get(i)))
            {
               sb.append(val);
            }
            else
            {
               sb.append(val);
               if (i == 0)
               {
                  Map<Integer, List<String>> m = activePKs.get(tableName);

                  if (m == null)
                     m = new HashMap<>();

                  List<String> gen = m.get(Integer.valueOf(0));

                  if (gen == null)
                     gen = new ArrayList<>(getRows(tableName));

                  gen.add(val);
                  m.put(Integer.valueOf(0), gen);
                  activePKs.put(tableName, m);
               }
            }
            if (!"NULL".equals(val) && mustEscape(colTypes.get(i)))
               sb.append("\'");
            if (i < colTypes.size() - 1)
               sb.append(", ");
         }
         sb.append(");");

         l.add(sb.toString());
      }

      return l;
   }

   /**
    * Write workload
    * @param profileName The name of the profile
    */
   private static void writeWorkload(String profileName) throws Exception
   {
      int clients = profile.getProperty("clients") != null ?
         Integer.parseInt(profile.getProperty("clients")) : DEFAULT_CLIENTS;
      int statements = profile.getProperty("statements") != null ?
         Integer.parseInt(profile.getProperty("statements")) : DEFAULT_STATEMENTS;
      int mspt = profile.getProperty("mspt") != null ?
         Integer.parseInt(profile.getProperty("mspt")) : DEFAULT_MAX_STATEMENTS_PER_TRANSACTION;
      int mixSelect = profile.getProperty("mix.select") != null ?
         Integer.parseInt(profile.getProperty("mix.select")) : DEFAULT_MIX_SELECT;
      int mixUpdate = profile.getProperty("mix.update") != null ?
         Integer.parseInt(profile.getProperty("mix.update")) : DEFAULT_MIX_UPDATE;
      int mixInsert = profile.getProperty("mix.insert") != null ?
         Integer.parseInt(profile.getProperty("mix.insert")) : DEFAULT_MIX_INSERT;
      int mixDelete = profile.getProperty("mix.delete") != null ?
         Integer.parseInt(profile.getProperty("mix.delete")) : DEFAULT_MIX_DELETE;
      int commit = profile.getProperty("commit") != null ?
         Integer.parseInt(profile.getProperty("commit")) : DEFAULT_COMMIT;
      int rollback = profile.getProperty("rollback") != null ?
         Integer.parseInt(profile.getProperty("rollback")) : DEFAULT_ROLLBACK;

      int totalSelect = 0;
      int totalUpdate = 0;
      int totalInsert = 0;
      int totalDelete = 0;
      int[] totalDistTx = new int[mspt];
      int totalTx = 0;
      int totalTxC = 0;
      int totalTxR = 0;
      int select = 0;
      int update = 0;
      int insert = 0;
      int delete = 0;
      int tx = 0;
      int txC = 0;
      int txR = 0;
      int[] distTx = new int[mspt];

      List<String> tableNames = new ArrayList<>(columnNames.size());
      for (String tableName : columnNames.keySet())
      {
         tableNames.add(tableName);
      }
      
      for (int i = 1; i <= clients; i++)
      {
         select = 0;
         update = 0;
         insert = 0;
         delete = 0;
         tx = 0;
         txC = 0;
         txR = 0;
         distTx = new int[mspt];

         int cStatements = profile.getProperty("client." + i + ".statements") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".statements")) : statements;
         int cMixSelect = profile.getProperty("client." + i + ".mix.select") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.select")) : mixSelect;
         int cMixUpdate = profile.getProperty("client." + i + ".mix.update") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.update")) : mixUpdate;
         int cMixInsert = profile.getProperty("client." + i + ".mix.insert") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.insert")) : mixInsert;
         int cMixDelete = profile.getProperty("client." + i + ".mix.delete") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".mix.delete")) : mixDelete;
         int cCommit = profile.getProperty("client." + i + ".commit") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".commit")) : commit;
         int cRollback = profile.getProperty("client." + i + ".rollback") != null ?
            Integer.parseInt(profile.getProperty("client." + i + ".rollback")) : rollback;

         List<String> l = new ArrayList<>();
         int statement = 0;
         while (statement <= cStatements)
         {
            int numberOfStatements = random.nextInt(mspt + 1);
            if (numberOfStatements == 0)
               numberOfStatements = 1;

            l.add("P");
            l.add("BEGIN");
            l.add("");
            l.add("");
            totalTx++;
            tx++;

            distTx[numberOfStatements - 1]++;
            totalDistTx[numberOfStatements - 1]++;

            for (int s = 0; s < numberOfStatements; s++)
            {
               String table = tableNames.get(random.nextInt(tableNames.size()));
               List<String> colNames = columnNames.get(table);
               List<String> colTypes = columnTypes.get(table);
               int type = 0;
               List<String> result = null;

               int tMixSelect = profile.getProperty(table + ".mix.select") != null ?
                  Integer.parseInt(profile.getProperty(table + ".mix.select")) : cMixSelect;
               int tMixUpdate = profile.getProperty(table + ".mix.update") != null ?
                  Integer.parseInt(profile.getProperty(table + ".mix.update")) : cMixUpdate;
               int tMixInsert = profile.getProperty(table + ".mix.insert") != null ?
                  Integer.parseInt(profile.getProperty(table + ".mix.insert")) : cMixInsert;
               int tMixDelete = profile.getProperty(table + ".mix.delete") != null ?
                  Integer.parseInt(profile.getProperty(table + ".mix.delete")) : cMixDelete;

               int size = tMixSelect + tMixUpdate + tMixInsert + tMixDelete;
               int[] tDistribution = new int[size];
               for (int ti = 0; ti < tMixSelect; ti++)
               {
                  tDistribution[ti] = 0;
               }
               for (int ti = 0; ti < tMixUpdate; ti++)
               {
                  tDistribution[ti + tMixSelect] = 1;
               }
               for (int ti = 0; ti < tMixInsert; ti++)
               {
                  tDistribution[ti + tMixSelect + tMixUpdate] = 2;
               }
               for (int ti = 0; ti < tMixDelete; ti++)
               {
                  tDistribution[ti + tMixSelect + tMixUpdate + tMixDelete] = 3;
               }

               type = tDistribution[random.nextInt(cMixSelect + cMixUpdate + cMixInsert + cMixDelete)];

               if (type == 1 && colNames.size() <= 1)
                  type = 0;

               if ((type == 0 && cMixSelect == 0) ||
                   (type == 1 && cMixUpdate == 0) ||
                   (type == 2 && cMixInsert == 0) ||
                   (type == 3 && cMixDelete == 0))
               {
                  type = -1;
               }

               switch (type)
               {
                  case 0:
                  {
                     result = generateSELECT(i, table, colNames, colTypes);
                     totalSelect++;
                     select++;
                     break;
                  }
                  case 1:
                  {
                     result = generateUPDATE(i, table, colNames, colTypes);
                     totalUpdate++;
                     update++;
                     break;
                  }
                  case 2:
                  {
                     result = generateINSERT(i, table, colNames, colTypes);
                     totalInsert++;
                     insert++;
                     break;
                  }
                  case 3:
                  {
                     result = generateDELETE(i, table, colNames, colTypes);
                     totalDelete++;
                     delete++;
                     break;
                  }
                  default:
                  {
                     result = null;
                     break;
                  }
               }

               if (result != null)
               {
                  l.add("P");
                  l.add(result.get(0));
                  l.add(result.get(1));
                  l.add(result.get(2));
               }
            }

            l.add("P");
            int c = random.nextInt(cCommit + cRollback + 1);
            if (c <= cCommit)
            {
               l.add("COMMIT");
               totalTxC++;
               txC++;
            }
            else
            {
               l.add("ROLLBACK");
               totalTxR++;
               txR++;
            }
            l.add("");
            l.add("");

            statement += numberOfStatements + 2;
         }

         writeFile(Paths.get(profileName, i + ".cli"), l);

         System.out.println("Client: " + i);
         System.out.println("      TX: " + tx + " (" + txC + "/" + txR + ")");
         System.out.println("                 " + Arrays.toString(distTx));
         System.out.println("  SELECT: " + select);
         System.out.println("  UPDATE: " + update);
         System.out.println("  INSERT: " + insert);
         System.out.println("  DELETE: " + delete);
      }

      System.out.println("Total: ");
      System.out.println("      TX: " + totalTx + " (" + totalTxC + "/" + totalTxR + ")");
      System.out.println("                 " + Arrays.toString(totalDistTx));
      System.out.println("  SELECT: " + totalSelect);
      System.out.println("  UPDATE: " + totalUpdate);
      System.out.println("  INSERT: " + totalInsert);
      System.out.println("  DELETE: " + totalDelete);
   }

   /**
    * Generate SELECT statement
    */
   private static List<String> generateSELECT(int client, String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int index = 0;
      Set<String> fks = foreignKeys.get(table);

      if (pk != null)
         index = colNames.indexOf(pk);

      StringBuilder sql = new StringBuilder();
      if (fks == null || fks.isEmpty())
      {
         sql.append("SELECT ");
         for (int col = 0; col < colNames.size(); col++)
         {
            sql.append(colNames.get(col));
            if (col < colNames.size() - 1)
               sql.append(", ");
         }
         sql.append(" FROM ");
         sql.append(table);
         sql.append(" WHERE ");
         sql.append(colNames.get(index));
         sql.append(" = ?");
      }
      else
      {
         List<String> tcs = new ArrayList<>();
         List<String> fkTables = new ArrayList<>();
         List<String> fkCols = new ArrayList<>();

         for (String fk : fks)
         {
            int index1 = fk.indexOf(":");
            int index2 = fk.lastIndexOf(":");
            tcs.add(fk.substring(0, index1));
            fkTables.add(fk.substring(index1 + 1, index2));
            fkCols.add(fk.substring(index2 + 1));
         }

         sql.append("SELECT ");
         for (int col = 0; col < colNames.size(); col++)
         {
            sql.append(table);
            sql.append("_.");
            sql.append(colNames.get(col));
            sql.append(" AS ");
            sql.append(table);
            sql.append("_");
            sql.append(colNames.get(col));
            sql.append(", ");
         }

         for (int i = 0; i < tcs.size(); i++)
         {
            String fkt = fkTables.get(i);
            List<String> fkNames = columnNames.get(fkt);
            for (int col = 0; col < fkNames.size(); col++)
            {
               sql.append(tcs.get(i));
               sql.append("_");
               sql.append(fkt);
               sql.append("_.");
               sql.append(fkNames.get(col));
               sql.append(" AS ");
               sql.append(tcs.get(i));
               sql.append("_");
               sql.append(fkt);
               sql.append("_");
               sql.append(fkNames.get(col));
               if (col < fkNames.size() - 1 || i < tcs.size() - 1)
                  sql.append(", ");
            }
         }

         sql.append(" FROM ");
         sql.append(table);
         sql.append(" ");
         sql.append(table);
         sql.append("_ ");

         for (int i = 0; i < tcs.size(); i++)
         {
            String tc = tcs.get(i);
            String fkt = fkTables.get(i);
            String fkc = fkCols.get(i);
            sql.append("LEFT OUTER JOIN ");
            sql.append(fkt);
            sql.append(" ");
            sql.append(tc);
            sql.append("_");
            sql.append(fkt);
            sql.append("_ ON ");
            sql.append(table);
            sql.append("_.");
            sql.append(tc);
            sql.append(" = ");
            sql.append(tc);
            sql.append("_");
            sql.append(fkt);
            sql.append("_.");
            sql.append(fkc);

            if (i < tcs.size() - 1)
               sql.append(" ");
         }

         sql.append(" WHERE ");
         sql.append(table);
         sql.append("_.");
         sql.append(colNames.get(index));
         sql.append(" = ?");
      }
               
      result.add(sql.toString());
      result.add(getJavaType(colTypes.get(index)));
      if (pk != null)
      {
         result.add(getPrimaryKey(client, table));
      }
      else
      {
         result.add(getData(table, colNames.get(index), colTypes.get(index), random.nextInt(getRows(table))));
      }

      return result;
   }

   /**
    * Generate UPDATE statement
    */
   private static List<String> generateUPDATE(int client, String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int index = 0;
      int rows = getRows(table);
      boolean[] updated = new boolean[colNames.size()];
      boolean proceed = false;

      if (pk != null)
         index = colNames.indexOf(pk);

      StringBuilder sql = new StringBuilder();
      StringBuilder types = new StringBuilder();
      StringBuilder values = new StringBuilder();

      sql.append("UPDATE ");
      sql.append(table);
      sql.append(" SET ");
      for (int col = 0; col < colNames.size(); col++)
      {
         updated[col] = false;
         if (pk == null && col != 0)
         {
            if (!isForeignKey(table, colNames.get(col)) && !isUnique(table, colNames.get(col)))
            {
               if (col > 0)
               {
                  for (int test = 0; test < col; test++)
                  {
                     if (updated[test])
                     {
                        sql.append(", ");
                        types.append("|");
                        values.append("|");
                        break;
                     }
                  }
               }

               sql.append(colNames.get(col));
               sql.append(" = ?");

               types.append(getJavaType(colTypes.get(col)));

               values.append(getData(table, colNames.get(col), colTypes.get(col), random.nextInt(rows)));

               updated[col] = true;
               proceed = true;
            }
         }
         else
         {
            if (col != index)
            {
               if (!isForeignKey(table, colNames.get(col)) && !isUnique(table, colNames.get(col)))
               {
                  if (col > 0)
                  {
                     for (int test = 0; test < col; test++)
                     {
                        if (updated[test])
                        {
                           sql.append(", ");
                           types.append("|");
                           values.append("|");
                           break;
                        }
                     }
                  }

                  sql.append(colNames.get(col));
                  sql.append(" = ?");

                  types.append(getJavaType(colTypes.get(col)));

                  values.append(getData(table, colNames.get(col), colTypes.get(col), random.nextInt(rows)));

                  updated[col] = true;
                  proceed = true;
               }
            }
         }
      }
      sql.append(" WHERE ");
      sql.append(colNames.get(index));
      sql.append(" = ?");
               
      result.add(sql.toString());

      if (types.length() > 0)
         types.append("|");
      types.append(getJavaType(colTypes.get(index)));

      result.add(types.toString());

      if (values.length() > 0)
         values.append("|");
      if (pk != null)
      {
         values.append(getPrimaryKey(client, table));
      }
      else
      {
         values.append(getData(table, colNames.get(index), colTypes.get(index), random.nextInt(rows)));
      }
      
      result.add(values.toString());

      if (!proceed)
         return null;

      return result;
   }

   /**
    * Generate INSERT statement
    */
   private static List<String> generateINSERT(int client, String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int rows = getRows(table);

      StringBuilder sql = new StringBuilder();
      StringBuilder types = new StringBuilder();
      StringBuilder values = new StringBuilder();

      sql.append("INSERT INTO ");
      sql.append(table);
      sql.append(" (");
      for (int i = 0; i < colNames.size(); i++)
      {
         sql.append(colNames.get(i));
         if (i < colNames.size() - 1)
            sql.append(", ");
      }
      sql.append(") VALUES (");
      for (int i = 0; i < colTypes.size(); i++)
      {
         String colType = colTypes.get(i);
         sql.append("?");
         types.append(getJavaType(colType));
         if (isPrimaryKey(table, colNames.get(i)))
         {
            values.append(generatePrimaryKey(client, table, colNames.get(i), colType));
         }
         else if (isForeignKey(table, colNames.get(i)))
         {
            values.append(generateForeignKey(client, table, colNames.get(i)));
         }
         else if (isUnique(table, colNames.get(i)))
         {
            values.append(generateUnique(table, colNames.get(i), colType));
         }
         else
         {
            values.append(getData(table, colNames.get(i), colType, random.nextInt(rows)));
         }
         if (i < colTypes.size() - 1)
         {
            sql.append(", ");
            types.append("|");
            values.append("|");
         }
      }
      sql.append(")");
               
      result.add(sql.toString());
      result.add(types.toString());
      result.add(values.toString());

      return result;
   }

   /**
    * Generate DELETE statement
    */
   private static List<String> generateDELETE(int client, String table, List<String> colNames, List<String> colTypes)
      throws Exception
   {
      List<String> result = new ArrayList<>(3);
      String pk = primaryKeys.get(table);
      int index = 0;
      int rows = getRows(table);
      boolean ret = true;

      if (toFrom.containsKey(table))
         return null;

      if (pk != null)
         index = colNames.indexOf(pk);

      StringBuilder sql = new StringBuilder();
      sql.append("DELETE FROM ");
      sql.append(table);
      sql.append(" WHERE ");
      sql.append(colNames.get(index));
      sql.append(" = ?");
               
      result.add(sql.toString());
      result.add(getJavaType(colTypes.get(index)));
      if (pk != null)
      {
         String value = getPrimaryKey(client, table);
         ret = deletePrimaryKey(client, table, value);
         result.add(value);
      }
      else
      {
         result.add(getData(table, colNames.get(index), colTypes.get(index), random.nextInt(rows)));
      }

      if (!ret)
         return null;

      return result;
   }
   
   /**
    * Verify type
    * @param type The type
    */
   private static void verifyType(String type) throws Exception
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
         case "bigserial":
         case "serial8":
         case "bit":
         case "bit varying":
         case "varbit":
         case "boolean":
         case "bool":
         case "bytea":
         case "character":
         case "char":
         case "character varying":
         case "varchar":
         case "date":
         case "double precision":
         case "float8":
         case "integer":
         case "int":
         case "int4":
         case "numeric":
         case "decimal":
         case "real":
         case "float4":
         case "smallint":
         case "int2":
         case "smallserial":
         case "serial2":
         case "serial":
         case "serial4":
         case "text":
         case "time":
         case "time without time zone":
         case "time with time zone":
         case "timetz":
         case "timestamp":
         case "timestamp without time zone":
         case "timestamp with time zone":
         case "timestamptz":
         case "uuid":
            break;
         default:
            throw new Exception("Unknown type: " + type);
      }
   }

   /**
    * Get Java type
    * @param type The type
    */
   private static String getJavaType(String type) throws Exception
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
            return Integer.toString(Types.BIGINT);
            //case "bigserial":
            //case "serial8":
         case "bit":
         case "bit varying":
         case "varbit":
            return Integer.toString(Types.BIT);
         case "boolean":
         case "bool":
            return Integer.toString(Types.BOOLEAN);
         case "bytea":
            return Integer.toString(Types.BINARY);
         case "character":
         case "char":
            return Integer.toString(Types.CHAR);
         case "character varying":
         case "varchar":
            return Integer.toString(Types.VARCHAR);
         case "date":
            return Integer.toString(Types.DATE);
         case "double precision":
         case "float8":
            return Integer.toString(Types.DOUBLE);
         case "integer":
         case "int":
         case "int4":
            return Integer.toString(Types.INTEGER);
         case "numeric":
            return Integer.toString(Types.NUMERIC);
         case "decimal":
            return Integer.toString(Types.DECIMAL);
         case "real":
         case "float4":
            return Integer.toString(Types.REAL);
         case "smallint":
         case "int2":
            return Integer.toString(Types.SMALLINT);
            //case "smallserial":
            //case "serial2":
            //case "serial":
            //case "serial4":
         case "text":
            return Integer.toString(Types.VARCHAR);
         case "time":
         case "time without time zone":
            return Integer.toString(Types.TIME);
         case "time with time zone":
         case "timetz":
            return Integer.toString(Types.TIME_WITH_TIMEZONE);
         case "timestamp":
         case "timestamp without time zone":
            return Integer.toString(Types.TIMESTAMP);
         case "timestamp with time zone":
         case "timestamptz":
            return Integer.toString(Types.TIMESTAMP_WITH_TIMEZONE);
         case "uuid":
            return Integer.toString(Types.OTHER);
      }
      throw new Exception("Unsupported type: " + type);
   }

   /**
    * Is primary key
    * @param table The table
    * @param col The col
    * @return True if primary key, otherwise false
    */
   private static boolean isPrimaryKey(String table, String col)
   {
      String pk = primaryKeys.get(table);

      if (pk == null)
         return false;

      return pk.equals(col);
   }

   /**
    * Generate primary key
    * @param client The client
    * @param table The table
    * @param name The column name
    * @param type The type
    */
   private static String generatePrimaryKey(int client, String table, String name, String type) throws Exception
   {
      return generatePrimaryKey(client, table, name, type, random.nextInt(Integer.MAX_VALUE));
   }

   /**
    * Generate primary key
    * @param client The client
    * @param table The table
    * @param name The column name
    * @param type The type
    * @param row Hint for row number
    */
   private static String generatePrimaryKey(int client, String table, String name, String type, int row) throws Exception
   {
      return generatePrimaryKey(client, table, name, type, row, true);
   }

   /**
    * Generate primary key
    * @param client The client
    * @param table The table
    * @param name The column name
    * @param type The type
    * @param row Hint for row number
    * @param r Random
    */
   private static String generatePrimaryKey(int client, String table, String name, String type, int row, boolean r) throws Exception
   {
      String newpk = getData(table, name, type, row, r);
      Map<Integer, List<String>> m = activePKs.get(table);

      if (m == null)
         m = new HashMap<>();

      if (client == 0)
      {
         List<String> gen = m.get(Integer.valueOf(0));

         if (gen == null)
            gen = new ArrayList<>(getRows(table));

         if (isDataRandom(type))
         {
            while (gen.contains(newpk))
            {
               newpk = getData(table, name, type, row, r);
            }
         }

         gen.add(newpk);
         m.put(Integer.valueOf(0), gen);
      }
      else
      {
         List<String> gen = m.get(Integer.valueOf(0));
         List<String> apks = m.get(Integer.valueOf(client));

         if (apks == null)
            apks = new ArrayList<>(getRows(table));

         while (gen.contains(newpk) || apks.contains(newpk))
         {
            newpk = getData(table, name, type, row, r);
         }
         apks.add(newpk);
         m.put(Integer.valueOf(client), apks);
      }

      activePKs.put(table, m);

      return newpk;
   }

   /**
    * Get a random primary key
    * @param client The client
    * @param table The table
    * @return The primary key
    */
   private static String getPrimaryKey(int client, String table) throws Exception
   {
      Map<Integer, List<String>> m = activePKs.get(table);
      List<String> apks = m.get(Integer.valueOf(client));

      if (apks == null || apks.size() == 0)
         apks = m.get(Integer.valueOf(0));

      return apks.get(random.nextInt(apks.size()));
   }

   /**
    * Delete primary key
    * @param client The client
    * @param table The table
    * @param pk The primary key
    * @return True if the primary key was deleted, otherwise false
    */
   private static boolean deletePrimaryKey(int client, String table, String pk) throws Exception
   {
      Map<Integer, List<String>> m = activePKs.get(table);
      List<String> apks = m.get(Integer.valueOf(client));

      if (apks == null || apks.size() == 0)
         return false;

      apks.remove(pk);
      m.put(Integer.valueOf(client), apks);
      activePKs.put(table, m);
      return true;
   }

   /**
    * Is foreign key
    * @param table The table
    * @param col The col
    * @return True if foreign key, otherwise false
    */
   private static boolean isForeignKey(String table, String col)
   {
      Set<String> ts = foreignKeys.get(table);

      if (ts == null)
         return false;

      for (String data : ts)
      {
         String c = data.substring(0, data.indexOf(":"));
         if (c.equals(col))
            return true;
      }

      return false;
   }

   /**
    * Generate foreign key
    * @param table The table
    * @param col The column
    * @return The foreign key
    */
   private static String generateForeignKey(int client, String table, String col)
   {
      Set<String> ts = foreignKeys.get(table);

      for (String data : ts)
      {
         int index1 = data.indexOf(":");

         String c = data.substring(0, index1);
         if (c.equals(col))
         {
            int index2 = data.lastIndexOf(":");
            String fkTable = data.substring(index1 + 1, index2);
            String fkCol = data.substring(index2 + 1);

            Map<Integer, List<String>> m = activePKs.get(fkTable);
            List<String> apks = m.get(Integer.valueOf(client));

            if (apks == null || apks.size() == 0)
               apks = m.get(Integer.valueOf(0));

            return apks.get(random.nextInt(apks.size()));
         }
      }

      return "";
   }

   /**
    * Is nullable
    * @param table The table
    * @param col The col
    * @return True if NULL is supported, otherwise false
    */
   private static boolean isNullable(String table, String col)
   {
      if (isPrimaryKey(table, col) || isForeignKey(table, col))
         return false;

      Set<String> ts = notNulls.get(table);

      if (ts == null)
         return true;

      return !ts.contains(col);
   }

   /**
    * Get the NULL target for a table
    * @param table The table name
    * @return The value
    */
   private static int getNullTarget(String table)
   {
      int defaultNotNull = profile.getProperty("notnull") != null ?
         Integer.parseInt(profile.getProperty("notnull")) : DEFAULT_NOT_NULL;

      int notnull = profile.getProperty(table + ".notnull") != null ?
         Integer.parseInt(profile.getProperty(table + ".notnull")) : defaultNotNull;

      return notnull;
   }

   /**
    * Is unique
    * @param table The table
    * @param col The col
    * @return True if UNIQUE, otherwise false
    */
   private static boolean isUnique(String table, String col)
   {
      Map<String, Set<String>> m = uniques.get(table);

      if (m == null)
         return false;

      return m.containsKey(col);
   }

   /**
    * Generate unique value
    * @param table The table
    * @param name The column name
    * @param type The type
    */
   private static String generateUnique(String table, String name, String type) throws Exception
   {
      String val = getData(table, name, type, Integer.MAX_VALUE, true);
      Map<String, Set<String>> m = uniques.get(table);

      if (m == null)
         m = new HashMap<>();

      Set<String> values = m.get(name);

      if (values == null)
         values = new HashSet<>();

      while (values.contains(val))
      {
         val = getData(table, name, type, Integer.MAX_VALUE, true);
      }

      m.put(name, values);
      uniques.put(table, m);

      return val;
   }

   /**
    * Get rows for a table
    * @param table The table name
    * @return The number of rows
    */
   private static int getRows(String table)
   {
      int defaultRows = profile.getProperty("rows") != null ?
         Integer.parseInt(profile.getProperty("rows")) : DEFAULT_ROWS;

      int rows = profile.getProperty(table + ".rows") != null ?
         Integer.parseInt(profile.getProperty(table + ".rows")) : defaultRows;

      return (int)(scale * rows);
   }

   /**
    * Get data
    * @param table The table
    * @param name The column name
    * @param type The type
    * @param row Hint for row number
    */
   private static String getData(String table, String name, String type, int row) throws Exception
   {
      return getData(table, name, type, row, true);
   }

   /**
    * Get data
    * @param table The table
    * @param name The column name
    * @param type The type
    * @param row Hint for row number
    * @param r Should the data be random
    */
   private static String getData(String table, String name, String type, int row, boolean r) throws Exception
   {
      String validChars = "ABCDEFGHIJKLMNOPQRSTUVXWZabcdefghijklmnopqrstuvxwz0123456789";

      if (isNullable(table, name) && random.nextInt(101) > getNullTarget(table))
      {
         return "NULL";
      }

      String base = type;
      int size = 0;
      if (base.indexOf("(") != -1)
      {
         if (base.indexOf(",") == -1)
         {
            size = Integer.parseInt(base.substring(base.indexOf("(") + 1, base.indexOf(")")));
         }
         else
         {
            size = Integer.parseInt(base.substring(base.indexOf("(") + 1, base.indexOf(",")));
         }
         base = base.substring(0, base.indexOf("("));
      }
      switch (base.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
            if (r)
            {
               return Long.toString(random.nextLong());
            }
            else
            {
               return Long.toString(row);
            }
            //case "bigserial":
            //case "serial8":
         case "bit":
         case "bit varying":
         case "varbit":
         case "boolean":
         case "bool":
            return Boolean.toString(random.nextBoolean());
            //case "bytea":
         case "character":
         case "char":
            return Character.toString(validChars.charAt(random.nextInt(validChars.length())));
         case "character varying":
         case "varchar":
            if (size == 0)
               size = 16;
            StringBuilder sb = new StringBuilder(size);
            for (int i = 0; i < size; i++)
               sb.append(validChars.charAt(random.nextInt(validChars.length())));
            return sb.toString();
         case "date":
            return new java.sql.Date(System.currentTimeMillis()).toString();
         case "double precision":
         case "float8":
            if (r)
            {
               return Double.toString(random.nextDouble());
            }
            else
            {
               return Double.toString(row);
            }
         case "integer":
         case "int":
         case "int4":
            if (r)
            {
               return Integer.toString(random.nextInt(Integer.MAX_VALUE));
            }
            else
            {
               return Integer.toString(row);
            }
         case "numeric":
         case "decimal":
            if (r)
            {
               return new java.math.BigDecimal(random.nextInt(Integer.MAX_VALUE)).toPlainString();
            }
            else
            {
               return new java.math.BigDecimal(row).toPlainString();
            }
         case "real":
         case "float4":
            if (r)
            {
               return Float.toString(random.nextFloat());
            }
            else
            {
               return Float.toString(row);
            }
         case "smallint":
         case "int2":
            return Short.toString((short)random.nextInt(Short.MAX_VALUE));
            //case "smallserial":
            //case "serial2":
            //case "serial":
            //case "serial4":
         case "text":
            sb = new StringBuilder(256);
            for (int i = 0; i < 256; i++)
               sb.append(validChars.charAt(random.nextInt(validChars.length())));
            return sb.toString();
         case "time":
         case "time without time zone":
         case "time with time zone":
         case "timetz":
            return new java.sql.Time(System.currentTimeMillis()).toString();
         case "timestamp":
         case "timestamp without time zone":
         case "timestamp with time zone":
         case "timestamptz":
            return new java.sql.Timestamp(System.currentTimeMillis()).toString();
         case "uuid":
            return java.util.UUID.randomUUID().toString();
      }
      throw new Exception("Unsupported type: " + type);
   }

   /**
    * Is data random
    * @param type The type
    */
   private static boolean isDataRandom(String type)
   {
      String base = type;
      if (base.indexOf("(") != -1)
      {
         base = base.substring(0, base.indexOf("("));
      }
      switch (base.toLowerCase().trim())
      {
         case "bigint":
         case "int8":
         case "double precision":
         case "float8":
         case "integer":
         case "int":
         case "int4":
         case "numeric":
         case "decimal":
         case "real":
         case "float4":
            return false;
      }
      return true;
   }
   
   /**
    * Must escape
    * @param type The type
    * @return True if escape needed, otherwise false
    */
   private static boolean mustEscape(String type) throws Exception
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "character":
         case "char":
         case "character varying":
         case "varchar":
         case "date":
         case "text":
         case "time":
         case "time without time zone":
         case "time with time zone":
         case "timetz":
         case "timestamp":
         case "timestamp without time zone":
         case "timestamp with time zone":
         case "timestamptz":
         case "uuid":
            return true;
      }
      return false;
   }

   /**
    * Is BTREE index
    * @param type The column type
    * @return True if BTREE, otherwise false (HASH)
    */
   private static boolean isBTreeIndex(String type)
   {
      if (type.indexOf("(") != -1)
         type = type.substring(0, type.indexOf("("));
      switch (type.toLowerCase().trim())
      {
         case "character varying":
         case "varchar":
         case "text":
            return false;
      }

      return true;
   }

   /**
    * Setup
    * @param name The name of the profile
    */
   private static void setup(String name) throws Exception
   {
      File p = new File(name);
      if (p.exists())
      {
         Files.walk(Paths.get(name))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
      p.mkdir();
   }

   /**
    * Main
    * @param args The arguments
    */
   public static void main(String[] args)
   {
      try
      {
         if (args.length == 1 || args.length > 4)
         {
            System.out.println("Usage: SQLLoadGenerator [-s scale] [-c configuration.properties]");
            return;
         }
         
         String s = DEFAULT_PROFILE;
         if (args.length >= 1)
         {
            if (!"-c".equals(args[0]) && !"-s".equals(args[0]))
               throw new Exception("Unknown option: " + args[0]);
         }
         if (args.length >= 2)
         {
            for (int parameter = 0; parameter < args.length - 1; parameter++)
            {
               if ("-s".equals(args[parameter]))
               {
                  scale = Double.valueOf(args[++parameter]);
               }
               else if ("-c".equals(args[parameter]))
               {
                  s = args[++parameter];
                  if (s.endsWith(".properties"))
                     s = s.substring(0, s.lastIndexOf("."));
               }
            }
         }

         profile = new Properties();
         InputStream input = new FileInputStream(s + ".properties");
         profile.load(input);
         input.close();
         
         setup(s);
         writeDDL(s);
         writeData(s);
         writeWorkload(s);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
