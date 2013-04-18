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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.junit.Before;

import com.cloudera.sqoop.hcat.HCatalogTestUtils.ColumnGenerator;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Test that we can export HCatalog tables into databases.
 */
public class HCatalogImportManualTest extends ImportJobTestCase {
  private static final Log LOG =
    LogFactory.getLog(HCatalogImportManualTest.class);
  private final HCatalogTestUtils utils = HCatalogTestUtils.instance();

  @Override
  @Before
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

  @Override
  protected List<String> getExtraArgs(Configuration conf) {
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
    addlArgsArray.add(getTableName());
    for (String k : addlArgsMap.keySet()) {
      if (!k.equals("-libjars")) {
        addlArgsArray.add(k);
        addlArgsArray.add(addlArgsMap.get(k));
      }
    }
    return addlArgsArray;
  }

  @Override
  protected String[] getArgv(boolean includeHadoopFlags, String[] colNames,
    Configuration conf) {
    if (null == colNames) {
      colNames = getColNames();
    }
    String columnsString = "";
    String splitByCol = null;
    if (colNames != null) {
      splitByCol = colNames[0];
      for (String col : colNames) {
        columnsString += col + ",";
      }
    }
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(getTableName());
    if (colNames != null) {
      args.add("--columns");
      args.add(columnsString);
      args.add("--split-by");
      args.add(splitByCol);
    }
    args.add("--hcatalog-table");
    args.add(getTableName());
    args.add("--connect");
    args.add(getConnectString());
    args.addAll(getExtraArgs(conf));

    return args.toArray(new String[0]);
  }

  private void validateHCatRecords(final List<HCatRecord> recs,
    final HCatSchema schema, int expectedCount,
    ColumnGenerator... cols) throws IOException {
    if (recs.size() != expectedCount) {
      fail("Expected records = " + expectedCount
        + ", actual = " + recs.size());
      return;
    }
    schema.getFieldNames();
    Collections.sort(recs, new Comparator<HCatRecord>()
    {
      @Override
      public int compare(HCatRecord hr1, HCatRecord hr2) {
        try {
          return hr1.getInteger("id", schema)
            - hr2.getInteger("id", schema);
        } catch (Exception e) {
          LOG.warn("Exception caught while sorting hcat records " + e);
        }
        return 0;
      }
    });

    Object expectedVal = null;
    Object actualVal = null;
    for (int i = 0; i < recs.size(); ++i) {
      HCatRecord rec = recs.get(i);
      expectedVal = i;
      actualVal = rec.get("id", schema);
      LOG.info("Validating field: id (expected = "
        + expectedVal + ", actual = " + actualVal + ")");
      utils.assertEquals(expectedVal, actualVal);
      expectedVal = "textfield" + i;
      actualVal = rec.get("msg", schema);
      LOG.info("Validating field: msg (expected = "
        + expectedVal + ", actual = " + actualVal + ")");
      utils.assertEquals(rec.get("msg", schema), "textfield" + i);
      for (ColumnGenerator col : cols) {
        String name = col.getName().toLowerCase();
        expectedVal = col.getHCatValue(i);
        actualVal = rec.get(name, schema);
        LOG.info("Validating field: " + name + " (expected = "
          + expectedVal + ", actual = " + actualVal + ")");
        utils.assertEquals(expectedVal, actualVal);
      }
    }
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
    HCatSchema tblSchema =
      utils.createHCatTable(true, TOTAL_RECORDS, table, cols);
    utils.createSqlTable(getConnection(), false, TOTAL_RECORDS, table, cols);
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
    String[] colNames = new String[2 + cols.length];
    colNames[0] = "ID";
    colNames[1] = "MSG";
    for (int i = 0; i < cols.length; ++i) {
      colNames[2 + i] = cols[i].getName().toUpperCase();
    }
    String[] importArgs = getArgv(true, colNames, new Configuration());
    LOG.debug("Import args = " + Arrays.toString(importArgs));
    runImport(importArgs);
    List<HCatRecord> recs = utils.readHCatRecords(null, table, null);
    LOG.debug("HCat records ");
    LOG.debug(utils.hCatRecordDump(recs, tblSchema));
    validateHCatRecords(recs, tblSchema, 10, cols);
  }
}
