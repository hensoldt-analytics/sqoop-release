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

package com.cloudera.sqoop.hcat;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.junit.Before;

import com.cloudera.sqoop.hcat.HCatalogTestUtils.ColumnGenerator;
import com.cloudera.sqoop.testutil.ExportJobTestCase;

/**
 * Test that we can export HCatalog tables into databases.
 */
public class HCatalogExportManualTest extends ExportJobTestCase {
  private static final Log LOG =
    LogFactory.getLog(HCatalogExportManualTest.class);
  private HCatalogTestUtils utils = HCatalogTestUtils.instance();
  @Before
  @Override
  public void setUp() {
    super.setUp();
    try {
      utils.initUtils();
    } catch (Exception e) {
      throw new RuntimeException("Error initializing HCatTestUtilis", e);
    }
  }
  /**
   * @return an argv for the CodeGenTool to use when creating tables to export.
   */
  protected String[] getCodeGenArgv(String... extraArgs) {
    List<String> codeGenArgv = new ArrayList<String>();

    if (null != extraArgs) {
      for (String arg : extraArgs) {
        codeGenArgv.add(arg);
      }
    }

    codeGenArgv.add("--table");
    codeGenArgv.add(getTableName());
    codeGenArgv.add("--connect");
    codeGenArgv.add(getConnectString());
    codeGenArgv.add("--hcatalog-table");
    codeGenArgv.add(getTableName());

    return codeGenArgv.toArray(new String[0]);
  }

  /**
   * Verify that for the max and min values of the 'id' column, the values for a
   * given column meet the expected values.
   */
  protected void assertColMinAndMax(String colName, ColumnGenerator generator)
    throws SQLException {
    Connection conn = getConnection();
    int minId = getMinRowId(conn);
    int maxId = getMaxRowId(conn);
    String table = getTableName();
    LOG.info("Checking min/max for column " + colName + " with type "
      + generator.getHiveType());

    Object expectedMin = generator.getDBValue(minId);
    Object expectedMax = generator.getDBValue(maxId);

    utils.assertSqlColValForRowId(conn, table, minId, colName, expectedMin);
    utils.assertSqlColValForRowId(conn, table, maxId, colName, expectedMax);
  }

  public void testSupportedHCatTypes() throws Exception {
    final int TOTAL_RECORDS = 1 * 10;
    ByteBuffer bb = ByteBuffer.wrap(new byte[] { 0, 1, 2 });
    String table = getTableName().toUpperCase();
    ColumnGenerator[] cols = new ColumnGenerator[] {
      utils.colGenerator(utils.forIdx(0), serdeConstants.BOOLEAN_TYPE_NAME,
        Types.BOOLEAN, HCatFieldSchema.Type.BOOLEAN, Boolean.TRUE,
        Boolean.TRUE),
      utils.colGenerator(utils.forIdx(1), serdeConstants.INT_TYPE_NAME,
        Types.INTEGER, HCatFieldSchema.Type.INT, 100, 100),
      utils.colGenerator(utils.forIdx(2), serdeConstants.BIGINT_TYPE_NAME,
        Types.BIGINT, HCatFieldSchema.Type.BIGINT, 200L, 200L),
      utils.colGenerator(utils.forIdx(3), serdeConstants.FLOAT_TYPE_NAME,
        Types.FLOAT, HCatFieldSchema.Type.FLOAT, 10.0F, 10.F),
      utils.colGenerator(utils.forIdx(4), serdeConstants.DOUBLE_TYPE_NAME,
        Types.DOUBLE, HCatFieldSchema.Type.DOUBLE, 20.0D, 20.0D),
      utils.colGenerator(utils.forIdx(5), serdeConstants.BINARY_TYPE_NAME,
        Types.BINARY, HCatFieldSchema.Type.BINARY, bb.array(), bb.array()),
    };
    utils.createHCatTable(false, TOTAL_RECORDS, table, cols);
    utils.createSqlTable(getConnection(), true, TOTAL_RECORDS, table, cols);
    List<String> addlArgsArray = new ArrayList<String>();
    Map<String, String> addlArgsMap = utils.getAddlTestArgs();
    String[] argv = {};

    if (addlArgsMap.containsKey("-libjars")) {
      argv = new String[2];
      argv[0] = "-libjars";
      argv[1] = addlArgsMap.get("-libjars");
    }
    addlArgsArray.add("-m");
    addlArgsArray.add("1");
    addlArgsArray.add("--hcatalog-table");
    addlArgsArray.add(table);
    for (String k : addlArgsMap.keySet()) {
      if (!k.equals("-libjars")) {
        addlArgsArray.add(k);
        addlArgsArray.add(addlArgsMap.get(k));
      }
    }
    String[] exportArgs = getArgv(true, 10, 10, newStrArray(argv,
      addlArgsArray.toArray(new String[0])));
    LOG.debug("Export args = " + Arrays.toString(exportArgs));
    runExport(exportArgs);
    verifyExport(TOTAL_RECORDS);
    for (int i = 0; i < cols.length; i++) {
      assertColMinAndMax(utils.forIdx(i), cols[i]);
    }
  }
}
