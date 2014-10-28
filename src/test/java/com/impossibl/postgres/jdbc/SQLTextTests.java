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
import java.text.ParseException;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SQLTextTests {

  String[][] sqlTransformTests = new String[][] {
    new String[] {

      //Input
      "select \"somthing\" -- This is a SQL comment ?WTF?\n" +
        " from\n" +
        "   test\n" +
        " where\n" +
        "   'a string with a ?' =  ?",

      //Output
      "select \"somthing\" -- This is a SQL comment ?WTF?\n" +
        " from\n" +
        "   test\n" +
        " where\n" +
        "   'a string with a ?' =  $1"
    },
    new String[] {

      //Input
      "insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
        " (a, \"b\", \"c\", \"d\")\n" +
        " values /* a nested\n" +
        " /* comment with  */ a ? */" +
        " (?,'a string with a ?', \"another ?\", ?, ?)",

      //Output
      "insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
        " (a, \"b\", \"c\", \"d\")\n" +
        " values /* a nested\n" +
        " /* comment with  */ a ? */" +
        " ($1,'a string with a ?', \"another ?\", $2, $3)",
    },
    new String[] {

      //Input
      "insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
        " (a, \"b\", \"c\", \"d\")\n" +
        " values /* a nested\n" +
        " /* comment with  */ a ? */" +
        " (?,'a string with a ?', \"another \"\" ?\", {fn concat('{fn '' some()}', {fn char(?)})}, ?)",

      //Output
      "insert into \"somthing\" -- This is a SQL comment ?WTF?\n" +
        " (a, \"b\", \"c\", \"d\")\n" +
        " values /* a nested\n" +
        " /* comment with  */ a ? */" +
        " ($1,'a string with a ?', \"another \"\" ?\", ('{fn '' some()}'||chr($2)), $3)",
    },
    new String[] {
      "select {fn abs(-10)} as absval, {fn user()}, {fn concat(x,y)} as val from {oj tblA left outer join tblB on x=y}",
      "select abs(-10) as absval, user, (x||y) as val from tblA left OUTER JOIN tblB ON x=y",
    }
  };

  /**
   * Tests transforming JDBC SQL input into PostgreSQL's wire
   * protocol format.
   * @throws SQLException
   * @throws ParseException
   *
   * @see SQLTextUtils.getProtocolSQLText
   */
  @Test
  public void testPostgreSQLText() throws SQLException, ParseException {

    for (String[] test : sqlTransformTests) {

      String expected = test[1];

      SQLText sqlText = new SQLText(test[0]);

      SQLTextEscapes.processEscapes(sqlText, null);

      assertThat(sqlText.toString(), is(equalTo(expected)));
    }
  }

  @Test
  public void testTruncate() throws SQLException, ParseException {
    String sql = "SELECT\n" +
        "      folder.entity_id  AS folder_id\n" +
        "    , archive_pers.entity_id AS person_id\n" +
        "/*\n" +
        "    , archive_pers.initials AS person_initials\n" +
        "*/\n" +
        "    ,ARRAY(WITH RECURSIVE t AS (SELECT\n" +
        "                                   1 AS level,\n" +
        "                                   p.entity_id,\n" +
        "                                   p.id,\n" +
        "                                   p.parent_id,\n" +
        "                                   p.name\n" +
        "                               FROM pants_krank_project p\n" +
        "                               WHERE p.entity_id = 1\n" +
        "                               UNION ALL\n" +
        "                               SELECT\n" +
        "                                   t.level + 1,\n" +
        "                                   c.entity_id,\n" +
        "                                   c.id,\n" +
        "                                   c.parent_id,\n" +
        "                                   c.name\n" +
        "                               FROM pants_krank_project c JOIN t ON c.id = t.parent_id)\n" +
        "          SELECT\n" +
        "              t.name\n" +
        "          FROM t\n" +
        "          ORDER BY level DESC)\n" +
        "      AS project_name_array\n" +
        "FROM\n" +
        "    fishy_email_delivery del JOIN fishy_email_folder_message fm ON fm.delivery_id = del.entity_id\n" +
        "    JOIN fishy_email_folder folder ON folder.entity_id = fm.folder_id\n" +
        "    JOIN pants_krank_entity ent ON ent.entity_id = folder.owner_id\n" +
        "    CROSS JOIN pants_krank_relation archive_comp\n" +
        "    LEFT OUTER JOIN pants_krank_person archive_pers ON archive_pers.entity_id = folder.owner_id\n" +
        "WHERE 1 = 1\n" +
        "      AND NOT EXISTS(SELECT * FROM\n" +
        "    fishy_email_folder ef JOIN fishy_email_mailbox em ON ef.entity_id = em.folder_id\n" +
        "WHERE ef.entity_id = folder.entity_id)\n" +
        "      AND folder.owner_id IN (\n" +
        "    SELECT\n" +
        "        archive_comp.entity_id\n" +
        "    WHERE archive_comp.entity_id = folder.owner_id\n" +
        "    UNION ALL SELECT\n" +
        "                  pers.entity_id\n" +
        "              FROM pants_krank_person pers\n" +
        "              WHERE pers.relation_id = archive_comp.entity_id AND folder.owner_id = pers.entity_id\n" +
        "    UNION ALL SELECT\n" +
        "                  pr.entity_id\n" +
        "              FROM pants_krank_project pr\n" +
        "              WHERE pr.relation_id = archive_comp.entity_id AND folder.owner_id = pr.entity_id\n" +
        "    UNION ALL SELECT\n" +
        "                  1\n" +
        "              FROM fishy_project_phase ph\n" +
        "                  JOIN fishy_project_phase_category cat ON 1 = cat.entity_id\n" +
        "                  JOIN pants_krank_project pr ON cat.project_id = pr.entity_id\n" +
        "              WHERE pr.relation_id = archive_comp.entity_id AND folder.owner_id = 3\n" +
        ")\n" +
        "      AND archive_comp.entity_id = 2\n" +
        "--          AND proj.entity_id = 890\n" +
        "ORDER BY del.received_timestamp DESC";

    SQLText sqlText = new SQLText(sql);
    SQLTextEscapes.processEscapes(sqlText, null);
  }
}
