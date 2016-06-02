/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestBooleanLiteralsSuite extends RegressionSuite {

    public void testBooleanLiteralsInWhere() throws Exception {
        System.out.println("\nSTARTING test having boolean literals in the \"WHERE\" clause...");
        Client client = getClient();
        VoltTable vt;
        client.callProcedure("T1Insert", 1);
        client.callProcedure("T1Insert", 3);
        client.callProcedure("T1Insert", 5);
        String[] conditions = {"1=1", "1=0", "TRUE", "FALSE", "1>2", "6-1>=0", "MOD(4,3)=1"};
        int[] resultCounts = {3, 0, 3, 0, 0, 3, 3};
        assertEquals(conditions.length, resultCounts.length);
        String sqlBody = "SELECT AINT FROM T1 WHERE %s";
        for (int i=0; i<conditions.length; i++) {
            String sql = String.format(sqlBody, conditions[i]);
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            assertEquals(vt.getRowCount(), resultCounts[i]);
        }
    }

    public TestBooleanLiteralsSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestBooleanLiteralsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        try {
            final String literalSchema =
                "CREATE TABLE T1 ( " +
                "AINT INTEGER DEFAULT 0 NOT NULL," +
                "PRIMARY KEY (AINT) );";
            project.addLiteralSchema(literalSchema);
            project.addStmtProcedure("T1Insert", "INSERT INTO T1 VALUES (?)");

            // CONFIG #1: Local Site/Partitions running on JNI backend
            config = new LocalCluster("bool-voltdbBackend.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            // alternative to enable for debugging */ config = new LocalCluster("IPC-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_IPC);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            // CONFIG #2: HSQL
            config = new LocalCluster("bool-hsqlBackend.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
        }
        catch(IOException excp) {
            assertFalse(true);
        }

        return builder;
    }
}
