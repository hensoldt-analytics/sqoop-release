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

package com.cloudera.sqoop;

import org.junit.Before;

import com.cloudera.sqoop.tool.ExportTool;
import com.cloudera.sqoop.tool.ImportTool;

import junit.framework.TestCase;

/**
 * Test basic HCatalog related features.
 */
public class TestHCatalogBasic extends TestCase {
  private static ImportTool importTool;
  private static ExportTool exportTool;

  @Before
  @Override
  public void setUp() {
    importTool = new ImportTool();
    exportTool = new ExportTool();
  }
  private SqoopOptions parseImportArgs(String[] argv) throws Exception {
    SqoopOptions opts = importTool.parseArguments(argv, null, null, false);
    return opts;
  }

  private SqoopOptions parseExportArgs(String[] argv) throws Exception {
    SqoopOptions opts = exportTool.parseArguments(argv, null, null, false);
    return opts;
  }

  public void testHCatalogHomeWithImport() throws Exception {
    String[] args = {
      "--hcatalog-home",
      "/usr/lib/hcatalog",
    };

    SqoopOptions opts = parseImportArgs(args);
  }

  public void testHCatalogHomeWithExport() throws Exception {
    String[] args = {
      "--hcatalog-home",
      "/usr/lib/hcatalog",
    };

    SqoopOptions opts = parseExportArgs(args);
  }

  public void testHCatalogImport() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
    };

    SqoopOptions opts = parseImportArgs(args);
  }

  public void testHCatalogExport() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
    };

    SqoopOptions opts = parseExportArgs(args);
  }

  public void testHCatImportWithTargetDir() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
      "--target-dir",
      "/target/dir",
    };
    try {
      SqoopOptions opts = parseImportArgs(args);
      importTool.validateOptions(opts);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testHCatImportWithWarehouseDir() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
      "--warehouse-dir",
      "/target/dir",
    };
    try {
      SqoopOptions opts = parseImportArgs(args);
      importTool.validateOptions(opts);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testHCatImportWithHiveImport() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
      "--hive-import",
    };
    try {
      SqoopOptions opts = parseImportArgs(args);
      importTool.validateOptions(opts);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testHCatExportWithExportDir() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
      "--export-dir",
      "/export/dir",
    };
    try {
      SqoopOptions opts = parseExportArgs(args);
      exportTool.validateOptions(opts);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testHCatImportWithDirect() throws Exception {
    String[] args = {
      "--hcatalog-table",
      "default.table",
      "--direct",
    };
    try {
      SqoopOptions opts = parseImportArgs(args);
      importTool.validateOptions(opts);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

}
