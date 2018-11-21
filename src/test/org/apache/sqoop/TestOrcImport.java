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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.sqoop.testutil.CommonArgs;
import org.apache.sqoop.testutil.HsqldbTestServer;
import org.apache.sqoop.testutil.ImportJobTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TimeZone;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests --as-orcfile.
 */
public class TestOrcImport extends ImportJobTestCase {

  private static final double DOUBLE_DELTA = 1e-10;

  public static final Log LOG = LogFactory
      .getLog(TestOrcImport.class.getName());

  /**
   * Create the argv to pass to Sqoop.
   *
   * @return the argv as an array of strings.
   */
  protected String[] getOutputArgv(boolean includeHadoopFlags,
          String[] extraArgs) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--m");
    args.add("1");
    args.add("--as-orcfile");
    if (extraArgs != null) {
      args.addAll(Arrays.asList(extraArgs));
    }

    return args.toArray(new String[args.size()]);
  }

  private Reader getResults() throws Exception {
    FileSystem fs = FileSystem.get(getConf());
    FileStatus[] stats = fs.listStatus(getTablePath());

    for (FileStatus stat : stats) {
      if (stat.getPath().getName().endsWith(".orc")) {
        return OrcFile.createReader(stat.getPath(), OrcFile.readerOptions(getConf()));
      }
    }
    throw new RuntimeException("Output ORC file not found.");
  }

  @Test
  public void testWeirdNames() throws Exception {
    String [] names = {"99numbers", "colname-with-dashes", "ⒻⓘⓡⓢⓣⓃⓐⓜⓔ"};
    String [] types = { "INT", "INT", "VARCHAR"};
    String [] vals = { "3", "9", "'ⒻⓘⓡⓢⓣⓃⓐⓜⓔ'"};
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Reader reader = getResults();
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1, batch.size);
    assertEquals(3, ((LongColumnVector)batch.cols[0]).vector[0]);
    assertEquals(9, ((LongColumnVector)batch.cols[1]).vector[0]);
    assertArrayEquals("ⒻⓘⓡⓢⓣⓃⓐⓜⓔ".getBytes("UTF-8"), ((BytesColumnVector) batch.cols[2]).vector[0]);
  }

  @Test
  public void testNullValues() throws Exception {
    String [] names = {"ti", "si", "i", "bi", "num", "dec", "double", "real", "float", "str", "dt", "ts"};
    String [] types = { "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "NUMERIC", "DECIMAL", "DOUBLE", "REAL", "FLOAT", "VARCHAR", "DATETIME", "TIMESTAMP"};
    String [] row1 = {"null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null"};
    String [] row2 = {"10", "10", "10", "10", "10", "10", "10", "10", "10", "'test'", "'2018-04-08'", "'2018-04-08 10:00:00'"};
    createTableWithColTypesAndNames(names, types, row1);
    insertIntoTable(names, types, row2);

    runImport(getOutputArgv(true, new String[] {"--map-column-hive", "TS=TIMESTAMP,DT=DATE"}));

    Reader reader = getResults();
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(2, batch.size);
    for (int i = 0; i < names.length; i ++) {
      assertTrue(batch.cols[i].isNull[0]);
    }
    for (int i = 0; i < names.length; i ++) {
      assertFalse(batch.cols[i].isNull[1]);
    }
  }

  @Test
  public void testNumericTypes() throws Exception {
    String [] names = {"ti", "si", "i", "bi", "num", "dec", "double", "real", "float"};
    String [] types = { "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "NUMERIC", "DECIMAL", "DOUBLE", "REAL", "FLOAT" };
    String [] vals = { "10", "10", "10", "10", "10", "10", "10", "10", "10" };
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Reader reader = getResults();
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1, batch.size);
    for (int i = 0; i < names.length; i ++) {
      if (batch.cols[i] instanceof LongColumnVector) {
        assertEquals(10, ((LongColumnVector)batch.cols[i]).vector[0]);
      } else {
        assertEquals(10, ((DoubleColumnVector) batch.cols[i]).vector[0], DOUBLE_DELTA);
      }
    }
  }

  @Test
  public void testStringTypes() throws Exception {
    String[] names = {"chr", "vchr", "clb"};
    String[] types = {"CHARACTER", "VARCHAR", "LONGVARCHAR"};
    String[] vals = {"'test'", "'test'", "'test'"};
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Reader reader = getResults();
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1, batch.size);
    for (int i = 0; i < names.length; i ++) {
      assertArrayEquals("test".getBytes(), ((BytesColumnVector) batch.cols[i]).vector[0]);
    }
  }

  /**
   * DATE, TIME and DATETIME Sql types are mapped to STRING in Hive imports
   * by default. ORC import currently mimics this behavior but might be
   * changed later.
   * @throws Exception
   */
  @Test
  public void testDatetimeTypesAreStoredAsString() throws Exception {
    String[] names = {"dt", "ts"};
    String[] types = {"DATETIME", "TIMESTAMP"};
    String[] vals = {"'2018-04-08'", "'2018-04-08 10:00:00'"};
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Reader reader = getResults();
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1, batch.size);
    assertArrayEquals("2018-04-08 00:00:00.0".getBytes(), ((BytesColumnVector) batch.cols[0]).vector[0]);
    assertArrayEquals("2018-04-08 10:00:00.0".getBytes(), ((BytesColumnVector) batch.cols[1]).vector[0]);
  }

  /**
   * Tests overriding type of Datetime columns with --map-column-hive.
   * @throws Exception
   */
  @Test
  public void testDatetimeTypeOverrides() throws Exception {
    TimeZone originalTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

      String[] names = {"dt", "ts"};
      String[] types = {"DATE", "TIMESTAMP"};
      String[] vals = {"'2018-04-08'", "'2018-04-08 10:00:00'"};
      createTableWithColTypesAndNames(names, types, vals);

      runImport(getOutputArgv(true, new String[]{"--map-column-hive", "TS=TIMESTAMP,DT=DATE"}));


      Reader reader = getResults();
      RecordReader rows = reader.rows();
      VectorizedRowBatch batch = reader.getSchema().createRowBatch();
      rows.nextBatch(batch);
      assertEquals(1, batch.size);
      assertEquals(17629, ((LongColumnVector) batch.cols[0]).vector[0]);
      assertEquals(1523181600000l, ((TimestampColumnVector) batch.cols[1]).time[0]);
      assertEquals(0, ((TimestampColumnVector) batch.cols[1]).nanos[0]);
    } finally {
      TimeZone.setDefault(originalTZ);
    }
  }

  @Test
  public void testBooleanType() throws Exception {
    String[] names = {"boot", "biit", "boof", "biif"};
    String[] types = {"BOOLEAN", "BIT", "BOOLEAN", "BIT"};
    String[] vals = {"true", "true", "false", "false"};
    createTableWithColTypesAndNames(names, types, vals);

    runImport(getOutputArgv(true, null));

    Reader reader = getResults();
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1, batch.size);
    assertEquals(1, ((LongColumnVector) batch.cols[0]).vector[0]);
    assertEquals(1, ((LongColumnVector) batch.cols[1]).vector[0]);
    assertEquals(0, ((LongColumnVector) batch.cols[2]).vector[0]);
    assertEquals(0, ((LongColumnVector) batch.cols[3]).vector[0]);
  }
}
