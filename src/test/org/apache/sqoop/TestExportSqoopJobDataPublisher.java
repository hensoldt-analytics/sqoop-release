/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ExportJobTestCase;
import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.SqoopTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.hcat.HCatalogTestUtils;
import org.junit.Test;

import java.io.File;
import java.sql.Types;
import java.util.ArrayList;

public class TestExportSqoopJobDataPublisher extends ExportJobTestCase {

    public static final Log LOG = LogFactory.getLog(TestExportSqoopJobDataPublisher.class.getName());
    private HCatalogTestUtils utils = HCatalogTestUtils.instance();

    public void setUp() {
        super.setUp();
        try {
            utils.initUtils();
        } catch (Exception e) {
            throw new RuntimeException("Error initializing HCatTestUtilis", e);
        }
    }

    public void tearDown() {
        super.tearDown();
    }

    /**
     * Create the argv to pass to Sqoop.
     * @return the argv as an array of strings.
     */
    protected String [] getArgv(boolean includeHadoopFlags, String [] moreArgs) {
        ArrayList<String> args = new ArrayList<String>();

        if (includeHadoopFlags) {
            CommonArgs.addHadoopFlags(args);
        }

        args.add("-D");
        args.add(ConfigurationConstants.DATA_PUBLISH_CLASS + "=" + DummyDataPublisher.class.getName());

        if (null != moreArgs) {
            for (String arg: moreArgs) {
                args.add(arg);
            }
        }

        args.add("--table");
        args.add(getTableName());
        args.add("--verbose");
        args.add("--hcatalog-table");
        args.add(getTableName());
        args.add("--connect");
        args.add(getConnectString());

        for (String a : args) {
            LOG.debug("ARG : "+ a);
        }

        return args.toArray(new String[0]);
    }

    private void runExportTest(HCatalogTestUtils.ColumnGenerator[] cols, String verificationScript, String [] args,
                               SqoopTool tool) throws Exception {

        // create a table and populate it with a row...
        utils.createHCatTable(HCatalogTestUtils.CreateMode.CREATE_AND_LOAD, 3, getTableName(), cols);

        // set up our mock hive shell to compare our generated script
        // against the correct expected one.
        SqoopOptions options = getSqoopOptions(args, tool);
        String hcatHome = options.getHCatHome();
        String testDataPath = new Path(new Path(hcatHome),
                "scripts/" + verificationScript).toString();
        System.setProperty("expected.script",
                new File(testDataPath).getAbsolutePath());

        // verify that we can import it correctly into hive.
        runExport(args);
    }

    private SqoopOptions getSqoopOptions(String [] args, SqoopTool tool) {
        SqoopOptions opts = null;
        try {
            opts = tool.parseArguments(args, null, null, true);
        } catch (Exception e) {
            fail("Invalid options: " + e.toString());
        }

        return opts;
    }

    /** Test that strings and ints are handled in the normal fashion. */
    @Test
    public void testHcatExport() throws Exception {
        final String TABLE_NAME = "NORMAL_HCAT_EXPORT";
        setCurTableName(TABLE_NAME);
        HCatalogTestUtils.ColumnGenerator[] cols = new HCatalogTestUtils.ColumnGenerator[] {
                HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(0),
                        "char(14)", Types.CHAR, HCatFieldSchema.Type.STRING, 0, 0,
                        "string to test", "string to test", HCatalogTestUtils.KeyType.NOT_A_KEY),
                HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(1),
                        "char(14)", Types.CHAR, HCatFieldSchema.Type.CHAR, 14, 0,
                        new HiveChar("string to test", 14), "string to test",
                        HCatalogTestUtils.KeyType.NOT_A_KEY),
                HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(2),
                        "char(14)", Types.CHAR, HCatFieldSchema.Type.VARCHAR, 14, 0,
                        new HiveVarchar("string to test", 14), "string to test",
                        HCatalogTestUtils.KeyType.NOT_A_KEY),
                HCatalogTestUtils.colGenerator(HCatalogTestUtils.forIdx(3),
                        "longvarchar", Types.LONGVARCHAR, HCatFieldSchema.Type.STRING, 0, 0,
                        "string to test", "string to test", HCatalogTestUtils.KeyType.NOT_A_KEY),
        };
        runExportTest(cols, "normalImport.q", getArgv(false, (String[])null), new ExportTool());
        assert (DummyDataPublisher.storeTable.equals("NORMAL_HCAT_EXPORT"));
        assert (DummyDataPublisher.storeType.equals("hsqldb"));
        assert (DummyDataPublisher.operation.equals("import"));
    }
}
