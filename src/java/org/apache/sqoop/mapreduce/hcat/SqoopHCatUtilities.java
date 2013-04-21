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

package org.apache.sqoop.mapreduce.hcat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hcatalog.shims.HCatHadoopShims;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.hive.HiveTypes;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.util.Executor;
import org.apache.sqoop.util.LoggingAsyncSink;

import com.cloudera.sqoop.SqoopOptions;

/**
 * Utility methods for the HCatalog support for Sqoop.
 */
public final class SqoopHCatUtilities {
  public static final String DEFHCATDB = "default";
  public static final String DEFHIVEHOME = "/usr/lib/hive";
  public static final String DEFHCATHOME = "/usr/lib/hcatalog";
  public static final String HIVESITEXMLPATH = "/conf/hive-site.xml";
  public static final String HCATSHAREDIR = "share/hcatalog";
  public static final String DEFLIBDIR = "lib";

  public static final String HCAT_DB_OUTPUT_COLTYPES_JAVA =
    "sqoop.hcat.db.output.coltypes.java";
  public static final String HCAT_DB_OUTPUT_COLTYPES_SQL =
    "sqoop.hcat.db.output.coltypes.sql";

  private static final String HCATCMD = Shell.WINDOWS ? "hcat.cmd" : "hcat";
  private SqoopOptions options;
  private ConnManager connManager;
  private String hCatTableName;
  private String hCatDatabaseName;
  private Configuration configuration;
  private Job hCatJob;
  private HCatSchema hCatOutputSchema;
  private HCatSchema hCatPartitionSchema;
  private HCatSchema projectedSchema;
  private boolean configured;

  private String hCatQualifiedTableName;
  private String hCatStaticPartitionKey;
  private List<String> hCatDynamicPartitionKeys;
  // DB stuff
  private String[] dbColumnNames;
  private String dbTableName;
  private Map<String, Integer> dbColumnTypes;

  private Map<String, Integer> externalColTypes;

  private int[] fieldPositions;
  private HCatSchema hCatFullTableSchema;
  private List<String> hCatFullTableSchemaFieldNames;

  // For testing support
  private static Class<? extends InputFormat> inputFormatClass =
    SqoopHCatExportFormat.class;

  private static Class<? extends OutputFormat> outputFormatClass =
    HCatOutputFormat.class;

  private static Class<? extends Mapper> exportMapperClass =
    SqoopHCatExportMapper.class;

  private static Class<? extends Mapper> importMapperClass =
    SqoopHCatImportMapper.class;

  private static Class<? extends Writable> importValueClass =
    DefaultHCatRecord.class;

  private static boolean testMode = false;

  /**
   * A class to hold the instance. For guaranteeing singleton creation using JMM
   * semantics.
   */
  public static final class Holder {
    @SuppressWarnings("synthetic-access")
    public static final SqoopHCatUtilities INSTANCE = new SqoopHCatUtilities();

    private Holder() {
    }
  }

  public static SqoopHCatUtilities instance() {
    return Holder.INSTANCE;
  }

  private SqoopHCatUtilities() {
    configured = false;
  }
  public static final Log LOG = LogFactory.getLog(
    SqoopHCatUtilities.class.getName());

  public boolean isConfigured() {
    return configured;
  }

  public void configureHCat(final SqoopOptions opts, final Job job,
    final ConnManager connMgr, final String dbTable,
    final String hcTable, final Configuration config)
    throws IOException {
    if (configured) {
      return;
    }
    if (testMode) {
      options = opts;
      configured = true;
      return;
    }
    options = opts;

    String home = opts.getHiveHome();

    if (home == null || home.length() == 0) {
      LOG.warn("Hive home is not set. job may fail if needed jar files "
        + "are not found correctly.  Please set HIVE_HOME in"
        + " sqoop-env.sh or provide --hive-home option.  Setting HIVE_HOME "
        + " to " + SqoopHCatUtilities.DEFHIVEHOME);
      home = SqoopHCatUtilities.DEFHIVEHOME;
    }

    home = opts.getHCatHome();
    if (home == null || home.length() == 0) {
      LOG
        .warn("HCatalog home is not set. job may fail if needed jar "
          + "files are not found correctly.  Please set HCAT_HOME in"
          + " sqoop-env.sh or provide --hcatalog-home option.  "
          + " Setting HCAT_HOME to "
          + SqoopHCatUtilities.DEFHCATHOME);
      home = SqoopHCatUtilities.DEFHCATHOME;
    }
    connManager = connMgr;
    dbTableName = dbTable;
    configuration = config;
    hCatJob = job;
    int indx = hcTable.indexOf('.');
    if (indx != -1) {
      hCatDatabaseName = hcTable.substring(0, indx);
      hCatTableName = hcTable.substring(indx + 1);
    } else {
      hCatDatabaseName = DEFHCATDB;
      hCatTableName = hcTable;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(hCatDatabaseName);
    sb.append('.').append(hCatTableName);
    hCatQualifiedTableName = sb.toString();

    String principalID =
      System.getProperty(HCatConstants.HCAT_METASTORE_PRINCIPAL);
    if (principalID != null) {
      configuration.set(HCatConstants.HCAT_METASTORE_PRINCIPAL,
        principalID);
    }
    hCatStaticPartitionKey = options.getHivePartitionKey();
    // Get the partition key filter if needed
    Map<String, String> filterMap = getHCatSPFilterMap();
    String filterStr = getHCatSPFilterStr();

    // For serializing the schema to conf
    HCatInputFormat hif =
      HCatInputFormat.setInput(hCatJob, hCatDatabaseName,
        hCatTableName);
    // For serializing the schema to conf
    if (filterStr != null) {
      LOG.debug("Setting hCatInputFormat filter to " + filterStr);
      hif.setFilter(filterStr);
    }

    hCatFullTableSchema =
      HCatInputFormat.getTableSchema(configuration);
    hCatFullTableSchemaFieldNames =
      hCatFullTableSchema.getFieldNames();

    if (filterMap != null) {
      LOG.debug("Setting hCatOutputFormat filter to " + filterStr);
    }

    HCatOutputFormat.setOutput(hCatJob,
      OutputJobInfo
        .create(hCatDatabaseName, hCatTableName, filterMap));
    hCatOutputSchema = HCatOutputFormat.getTableSchema(configuration);
    List<HCatFieldSchema> hCatPartitionSchemaFields =
      new ArrayList<HCatFieldSchema>();
    int totalFieldsCount = hCatFullTableSchema.size();
    int dataFieldsCount = hCatOutputSchema.size();
    if (totalFieldsCount > dataFieldsCount) {
      for (int i = dataFieldsCount; i < totalFieldsCount; ++i) {
        hCatPartitionSchemaFields.add(hCatFullTableSchema.get(i));
      }
    }

    hCatPartitionSchema = new HCatSchema(hCatPartitionSchemaFields);
    for (HCatFieldSchema hfs : hCatPartitionSchemaFields) {
      if (hfs.getType() != HCatFieldSchema.Type.STRING) {
        throw new IOException("The table provided "
          + getQualifiedHCatTableName()
          + " uses unsupported  partitioning key type  for column "
          + hfs.getName() + " : " + hfs.getTypeString() + ".  Only string "
          + "fields are allowed in partition columns in HCatalog");
      }

    }
    initDBColumnNamesAndTypes();

    List<HCatFieldSchema> outputFieldList = new ArrayList<HCatFieldSchema>();
    for (String col : dbColumnNames) {
      outputFieldList.add(hCatFullTableSchema.get(col.toLowerCase()));
    }

    projectedSchema = new HCatSchema(outputFieldList);
    validateStaticPartitionKey();
    validateFieldAndColumnMappings();
    validateHCatTableFieldTypes();
    HCatOutputFormat.setSchema(configuration, projectedSchema);
    addJars(hCatJob, options);
    configured = true;
  }

  public void validateDynamicPartitionKeysMapping() throws IOException {
    // Now validate all partition columns are in the database column list
    StringBuilder missingKeys = new StringBuilder();

    for (String s : hCatDynamicPartitionKeys) {
      boolean found = false;
      for (String c : dbColumnNames) {
        if (s.equalsIgnoreCase(c)) {
          found = true;
          break;
        }
      }
      if (!found) {
        missingKeys.append(',').append(s);
      }
    }
    if (missingKeys.length() > 0) {
      throw new IOException("Dynamic partition keys are not "
        + "present in the database columns.   Missing keys = "
        + missingKeys.substring(1));
    }
  }

  public void validateHCatTableFieldTypes() throws IOException {
    StringBuilder sb = new StringBuilder();
    boolean hasComplexFields = false;
    for (HCatFieldSchema hfs : projectedSchema.getFields()) {
      if (hfs.isComplex()) {
        sb.append('.').append(hfs.getName());
        hasComplexFields = true;
      }
    }

    if (hasComplexFields) {
      String unsupportedFields = sb.substring(1);
      throw new IOException("The HCatalog table provided "
        + getQualifiedHCatTableName() + " has complex field types ("
        + unsupportedFields + ").  They are not currently supported");
    }

  }

  /**
   * Get the column names to import.
   */
  private void initDBColumnNamesAndTypes() throws IOException {
    String[] colNames = options.getColumns();
    if (null != colNames) {
      dbColumnNames = colNames; // user-specified column names.
    } else if (null != externalColTypes) {
      // Test-injection column mapping. Extract the col names from
      ArrayList<String> keyList = new ArrayList<String>();
      for (String key : externalColTypes.keySet()) {
        keyList.add(key);
      }
      dbColumnNames = keyList.toArray(new String[keyList.size()]);
    } else if (null != dbTableName) {
      dbColumnNames =
        connManager.getColumnNames(dbTableName);
    } else if (options.getCall() != null) {
      // Read procedure arguments from metadata
      dbColumnNames = connManager.getColumnNamesForProcedure(
        this.options.getCall());
    } else {
      dbColumnNames =
        connManager.getColumnNamesForQuery(options.getSqlQuery());
    }
    Map<String, Integer> colTypes = null;
    if (externalColTypes != null) { // Use pre-defined column types.
      colTypes = externalColTypes;
    } else { // Get these from the database.
      if (dbTableName != null) {
        colTypes = connManager.getColumnTypes(dbTableName);
      } else if (options.getCall() != null) {
        // Read procedure arguments from metadata
        colTypes = connManager.getColumnTypesForProcedure(
          this.options.getCall());
      } else {
        colTypes = connManager.getColumnTypesForQuery(
          options.getSqlQuery());
      }
    }
    if (options.getColumns() == null) {
      dbColumnTypes = colTypes;
    } else {
      dbColumnTypes = new HashMap<String, Integer>();
      // prune column types based on projection
      for (String col : dbColumnNames) {
        Integer type = colTypes.get(col);
        if (type == null) {
          throw new IOException("Projected column " + col
            + " not in list of columns from database");
        }
        dbColumnTypes.put(col, type);
      }
    }
  }

  private void validateFieldAndColumnMappings() throws IOException {
    Properties userMapping = options.getMapColumnHive();
    // Check that all explicitly mapped columns are present
    for (Object column : userMapping.keySet()) {
      boolean found = false;
      for (String c : dbColumnNames) {
        if (c.equalsIgnoreCase((String) column)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException("Column " + column
          + " not found while mapping database columns to hcatalog columns");
      }
    }

    fieldPositions = new int[dbColumnNames.length];

    for (int indx = 0; indx < dbColumnNames.length; ++indx) {
      boolean userMapped = false;
      String col = dbColumnNames[indx];
      Integer colType = dbColumnTypes.get(col);
      String hCatField = col.toLowerCase();
      String hCatColType = userMapping.getProperty(hCatField);
      if (hCatColType == null) {
        LOG.debug("No type mapping for HCatalog filed " + hCatField);
        hCatColType = connManager.toHCatType(colType);
      } else {
        LOG.debug("Found type mapping for HCatalog filed " + hCatField);
        userMapped = true;
      }
      if (null == hCatColType) {
        throw new IOException("HCat does not support the SQL type for column "
          + col);
      }

      boolean found = false;
      for (String tf : hCatFullTableSchemaFieldNames) {
        if (tf.equalsIgnoreCase(col)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IOException("Database column " + col + " not found in "
          + "hcatalog table schema or partition schema");
      }
      if (!userMapped) {
        HCatFieldSchema hCatFS = hCatFullTableSchema.get(col.toLowerCase());
        if (!hCatFS.getTypeString().equalsIgnoreCase(hCatColType)) {
          throw new IOException("The HCatalog field " + col
            + " has type " + hCatFS.getTypeString() + ".  Expected = "
            + hCatColType + " based on database column type : "
            + sqlTypeString(colType));
        }
      }

      if (HiveTypes.isHiveTypeImprovised(colType)) {
        LOG.warn("Column " + col
          + " had to be cast to a less precise type " + hCatColType
          + " in hcatalog");
      }
      fieldPositions[indx] =
        hCatFullTableSchemaFieldNames.indexOf(col.toLowerCase());
      if (fieldPositions[indx] < 0) {
        throw new IOException("The HCatalog field " + col
          + " could not be found");
      }
    }

  }

  private String getHCatSPFilterStr() {
    if (hCatStaticPartitionKey != null) {
      StringBuilder filter = new StringBuilder();
      filter.append(options.getHivePartitionKey()).append('=')
        .append('\'').append(options.getHivePartitionValue())
        .append('\'');
      return filter.toString();
    }
    return null;
  }

  private Map<String, String> getHCatSPFilterMap() {
    if (hCatStaticPartitionKey != null) {
      Map<String, String> filter = new HashMap<String, String>();
      filter.put(options.getHivePartitionKey(),
        options.getHivePartitionValue());
      return filter;
    }
    return null;
  }

  private void validateStaticPartitionKey() throws IOException {
    // check the static partition key from command line
    List<HCatFieldSchema> partFields = hCatPartitionSchema.getFields();

    if (hCatStaticPartitionKey != null) {
      boolean found = false;
      for (HCatFieldSchema hfs : partFields) {
        if (hfs.getName().equalsIgnoreCase(hCatStaticPartitionKey)) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new IOException("The provided hive partition key "
          + hCatStaticPartitionKey + " is not part of the partition "
          + " keys for table " + getQualifiedHCatTableName());
      }
    }
    hCatDynamicPartitionKeys = new ArrayList<String>();
    hCatDynamicPartitionKeys.addAll(hCatPartitionSchema
      .getFieldNames());
    if (hCatStaticPartitionKey != null) {
      hCatDynamicPartitionKeys.remove(hCatStaticPartitionKey);
    }
  }


  public static void configureImportOutputFormat(SqoopOptions opts, Job job,
    ConnManager connMgr, String dbTable,
    String hcTable, Configuration config) throws IOException {
    if (testMode) {
      LOG.info("In test mode.  No configuration of HCatalog is done");
      return;
    }
    LOG.debug("Configuring HCatalog for import job");
    SqoopHCatUtilities.instance().configureHCat(opts, job,
      connMgr, dbTable, opts.getHCatTable(), job.getConfiguration());
    LOG.info("Validating dynamic partition keys");
    SqoopHCatUtilities.instance().validateDynamicPartitionKeysMapping();
    job.setOutputFormatClass(getOutputFormatClass());
  }

  public static void configureExportInputFormat(SqoopOptions opts, Job job,
    ConnManager connMgr, String dbTable,
    String hcTable, Configuration config) throws IOException {
    if (testMode) {
      LOG.info("In test mode.  No configuration of HCatalog is done");
      return;
    }
    LOG.debug("Configuring HCatalog for export job");
    SqoopHCatUtilities hCatUtils = SqoopHCatUtilities.instance();
    hCatUtils.configureHCat(opts, job, connMgr, dbTable,
      opts.getHCatTable(), job.getConfiguration());
    job.setInputFormatClass(getInputFormatClass());
    Map<String, Integer> dbColTypes = hCatUtils.getDbColumnTypes();
    MapWritable columnTypesJava = new MapWritable();
    for (Map.Entry<String, Integer> e : dbColTypes.entrySet()) {
      Text columnName = new Text(e.getKey());
      Text columnText = new Text(
        connMgr.toJavaType(dbTable, e.getKey(), e.getValue()));
      columnTypesJava.put(columnName, columnText);
    }
    MapWritable columnTypesSql = new MapWritable();
    for (Map.Entry<String, Integer> e : dbColTypes.entrySet()) {
      Text columnName = new Text(e.getKey());
      IntWritable sqlType = new IntWritable(e.getValue());
      columnTypesSql.put(columnName, sqlType);
    }
    DefaultStringifier.store(job.getConfiguration(), columnTypesJava,
      SqoopHCatUtilities.HCAT_DB_OUTPUT_COLTYPES_JAVA);
    DefaultStringifier.store(job.getConfiguration(), columnTypesSql,
      SqoopHCatUtilities.HCAT_DB_OUTPUT_COLTYPES_SQL);

  }

  /**
   * Add the Hive and HCatalog jar files to local classpath and dist cache.
   *
   * @throws IOException
   */
  public static void addJars(Job job, SqoopOptions options)
    throws IOException {

    if (isLocalJobTracker(job)) {
      LOG.info("Not adding hcatalog jars to distributed cache in local mode");
      return;
    }
    Configuration conf = job.getConfiguration();
    String hiveHome = null;
    String hCatHome = null;
    FileSystem fs = FileSystem.getLocal(conf);
    if (options != null) {
      hiveHome = options.getHiveHome();
    }
    if (hiveHome == null) {
      hiveHome = DEFHIVEHOME;
    }
    if (options != null) {
      hCatHome = options.getHCatHome();
    }
    if (hCatHome == null) {
      hCatHome = DEFHCATHOME;
    }
    LOG.info("HCatalog job : Hive Home = " + hiveHome);
    LOG.info("HCatalog job:  HCatalog Home = " + hCatHome);

    conf.addResource(hiveHome + HIVESITEXMLPATH);

    // Add these to the 'tmpjars' array, which the MR JobSubmitter
    // will upload to HDFS and put in the DistributedCache libjars.
    List<String> libDirs = new ArrayList<String>();
    libDirs.add(hCatHome + File.separator + HCATSHAREDIR);
    libDirs.add(hCatHome + File.separator + DEFLIBDIR);
    libDirs.add(hiveHome + File.separator + DEFLIBDIR);
    Set<String> localUrls = new HashSet<String>();
    // Add any libjars already specified
    localUrls.addAll(conf.
      getStringCollection(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM));
    for (String dir : libDirs) {
      LOG.info("Adding jar files under " + dir + " to distributed cache");
      addDirToCache(new File(dir), fs, localUrls, false);
    }

    // Recursively add all hcatalog storage handler jars
    // The HBase storage handler is getting deprecated post Hive+HCat merge
    String hCatStorageHandlerDir = hCatHome + File.separator
      + "share/hcatalog/storage-handlers";
    LOG.info("Adding jar files under " + hCatStorageHandlerDir
      + " to distributed cache (recursively)");

    addDirToCache(new File(hCatStorageHandlerDir), fs, localUrls, true);

    String tmpjars = conf.get(
      ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM);
    StringBuilder sb = new StringBuilder(1024);
    if (null != tmpjars) {
      sb.append(tmpjars);
      sb.append(",");
    }
    sb.append(StringUtils.arrayToString(localUrls.toArray(new String[0])));
    conf.set(ConfigurationConstants.MAPRED_DISTCACHE_CONF_PARAM,
      sb.toString());
  }

  /**
   * Add the .jar elements of a directory to the DCache classpath, optionally
   * recursively.
   */
  private static void addDirToCache(File dir, FileSystem fs,
    Set<String> localUrls, boolean recursive) {
    if (dir == null) {
      return;
    }

    File[] fileList = dir.listFiles();

    if (fileList == null) {
      LOG.warn("No files under " + dir
        + " to add to distributed cache for hcatalog job");
      return;
    }

    for (File libFile : dir.listFiles()) {
      if (libFile.exists() && !libFile.isDirectory()
        && libFile.getName().endsWith("jar")) {
        Path p = new Path(libFile.toString());
        if (libFile.canRead()) {
          String qualified = p.makeQualified(fs).toString();
          LOG.debug("Adding to job classpath: " + qualified);
          localUrls.add(qualified);
        } else {
          LOG.warn("Ignoring unreadable file " + libFile);
        }
      }
      if (recursive && libFile.isDirectory()) {
        addDirToCache(libFile, fs, localUrls, recursive);
      }
    }
  }

  public static boolean isHadoop23() {
    String version = org.apache.hadoop.util.VersionInfo.getVersion();
    if (version.matches("\\b0\\.23\\..+\\b")) {
      return true;
    }
    return false;
  }

  public static boolean isLocalJobTracker(Job job) {
    Configuration conf = job.getConfiguration();
    String jtAddr =
      conf.get(ConfigurationConstants.PROP_MAPRED_JOB_TRACKER_ADDRESS);
    String jtAddr2 =
      conf.get(ConfigurationConstants.PROP_MAPREDUCE_JOB_TRACKER_ADDRESS);
    return (jtAddr != null && jtAddr.equals("local"))
      || (jtAddr2 != null && jtAddr2.equals("local"));
  }

  public void invokeOutputCommitterForLocalMode(Job job) throws IOException {
    if (isLocalJobTracker(job) && !isHadoop23()) {
      LOG.debug("Explicitly committing job in local mode");
      HCatHadoopShims.Instance.get().commitJob(new HCatOutputFormat(), job);
    }
  }

  public void launchHCatCli(String cmdLine) throws IOException {
    String tmpFileName = null;
    // Create the argv for the HCatalog Cli Driver.
    String[] argArray = new String[2];
    argArray[0] = "-e";
    System.getProperty("java.io.tmpdir");
    if (options != null) {
      options.getTempDir();
    }
    tmpFileName = "hcat-script-" + System.currentTimeMillis();
    writeHCatScriptFile(tmpFileName, cmdLine);
    argArray[0] = "-f";
    argArray[1] = tmpFileName;
    LOG.debug("Using external HCatalog CLI process.");
    executeExternalHCatProgram(Executor.getCurEnvpStrings(), argArray);

  }

  public void writeHCatScriptFile(String fileName, String contents)
    throws IOException {
    BufferedWriter w = null;
    try {
      FileOutputStream fos = new FileOutputStream(fileName);
      w = new BufferedWriter(new OutputStreamWriter(fos));
      w.write(contents, 0, contents.length());
    } catch (IOException ioe) {
      LOG.error("Error writing HCatalog load-in script: " + ioe.toString());
      ioe.printStackTrace();
      throw ioe;
    } finally {
      if (null != w) {
        try {
          w.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing stream to HCatalog script: "
              + ioe.toString());
        }
      }
    }
  }

  /**
   * Execute HCat via an external 'bin/hcat' process.
   *
   *
   * @param env
   *          the environment strings to pass to any subprocess.
   * @throws IOException
   *           if HCatalog did not exit successfully.
   */
  public void executeExternalHCatProgram(List<String> env,
    String[] cmdLine) throws IOException {
    // run HCat command with the given args
    String hCatProgram = getHCatPath();
    ArrayList<String> args = new ArrayList<String>();
    args.add(hCatProgram);
    if (cmdLine != null && cmdLine.length > 0) {
      for (String s : cmdLine) {
        args.add(s);
      }
    }
    LoggingAsyncSink logSink = new LoggingAsyncSink(LOG);
    int ret = Executor.exec(args.toArray(new String[0]),
      env.toArray(new String[0]), logSink, logSink);
    if (0 != ret) {
      throw new IOException("HCat exited with status " + ret);
    }
  }

  /**
   * @return the filename of the hcat executable to run to do the import
   */
  public String getHCatPath() {
    String hCatHome = null;
    if (options == null) {
      hCatHome = System.getenv("HCAT_HOME");
      hCatHome = System.getProperty("hcatalog.home", hCatHome);
    } else {
      hCatHome = options.getHCatHome();
    }

    if (null == hCatHome) {
      return HCATCMD;
    }

    Path p = new Path(hCatHome);
    p = new Path(p, "bin");
    p = new Path(p, HCATCMD);
    String hCatBinStr = p.toString();
    if (new File(hCatBinStr).exists()) {
      return hCatBinStr;
    } else {
      return HCATCMD;
    }
  }

  public static boolean isTestMode() {
    return testMode;
  }

  public static void setTestMode(boolean mode) {
    testMode = mode;
  }

  public static Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  public static Class<? extends OutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  public static void setInputFormatClass(Class<? extends InputFormat> clz) {
    inputFormatClass = clz;
  }

  public static void setOutputFormatClass(Class<? extends OutputFormat> clz) {
    outputFormatClass = clz;
  }

  public static Class<? extends Mapper> getImportMapperClass() {
    return importMapperClass;
  }

  public static Class<? extends Mapper> getExportMapperClass() {
    return exportMapperClass;
  }

  public static void setExportMapperClass(Class<? extends Mapper> clz) {
    exportMapperClass = clz;
  }

  public static void setImportMapperClass(Class<? extends Mapper> clz) {
    importMapperClass = clz;
  }

  public static Class<? extends Writable> getImportValueClass() {
    return importValueClass;
  }

  public static void setImportValueClass(Class<? extends Writable> clz) {
    importValueClass = clz;
  }

  /**
   * Set the column type map to be used. (dependency injection for testing; not
   * used in production.)
   */
  public void setColumnTypes(Map<String, Integer> colTypes) {
    externalColTypes = colTypes;
    LOG.debug("Using test-controlled type map");
  }

  public String getDatabaseTable() {
    return dbTableName;
  }

  public String getHCatTableName() {
    return hCatTableName;
  }

  public String getHCatDatabaseName() {
    return hCatDatabaseName;
  }

  public String getQualifiedHCatTableName() {
    return hCatQualifiedTableName;
  }

  public List<String> getHCatDynamicPartitionKeys() {
    return hCatDynamicPartitionKeys;
  }

  public String getHCatStaticPartitionKey() {
    return hCatStaticPartitionKey;
  }

  public String[] getDBColumnNames() {
    return dbColumnNames;
  }

  public HCatSchema getHCatOutputSchema() {
    return hCatOutputSchema;
  }

  public void setHCatOutputSchema(HCatSchema schema) {
    hCatOutputSchema = schema;
  }

  public HCatSchema getHCatPartitionSchema() {
    return hCatPartitionSchema;
  }

  public void setHCatPartitionSchema(HCatSchema schema) {
    hCatPartitionSchema = schema;
  }

  public void setHCatStaticPartitionKey(String key) {
    hCatStaticPartitionKey = key;
  }


  public void
    setHCatDynamicPartitionKeys(List<String> keys) {
    hCatDynamicPartitionKeys = keys;
  }

  public String[] getDbColumnNames() {
    return dbColumnNames;
  }

  public void setDbColumnNames(String[] names) {
    dbColumnNames = names;
  }

  public Map<String, Integer> getDbColumnTypes() {
    return dbColumnTypes;
  }

  public void setDbColumnTypes(Map<String, Integer> types) {
    dbColumnTypes = types;
  }

  public String gethCatTableName() {
    return hCatTableName;
  }

  public String gethCatDatabaseName() {
    return hCatDatabaseName;
  }

  public String gethCatQualifiedTableName() {
    return hCatQualifiedTableName;
  }

  public void setConfigured(boolean value) {
    configured = value;
  }

  public static String sqlTypeString(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
        return "BIT";
      case Types.TINYINT:
        return "TINYINT";
      case Types.SMALLINT:
        return "SMALLINT";
      case Types.INTEGER:
        return "INTEGER";
      case Types.BIGINT:
        return "BIGINT";
      case Types.FLOAT:
        return "FLOAT";
      case Types.REAL:
        return "REAL";
      case Types.DOUBLE:
        return "DOUBLE";
      case Types.NUMERIC:
        return "NUMERIC";
      case Types.DECIMAL:
        return "DECIMAL";
      case Types.CHAR:
        return "CHAR";
      case Types.VARCHAR:
        return "VARCHAR";
      case Types.LONGVARCHAR:
        return "LONGVARCHAR";
      case Types.DATE:
        return "DATE";
      case Types.TIME:
        return "TIME";
      case Types.TIMESTAMP:
        return "TIMESTAMP";
      case Types.BINARY:
        return "BINARY";
      case Types.VARBINARY:
        return "VARBINARY";
      case Types.LONGVARBINARY:
        return "LONGVARBINARY";
      case Types.NULL:
        return "NULL";
      case Types.OTHER:
        return "OTHER";
      case Types.JAVA_OBJECT:
        return "JAVA_OBJECT";
      case Types.DISTINCT:
        return "DISTINCT";
      case Types.STRUCT:
        return "STRUCT";
      case Types.ARRAY:
        return "ARRAY";
      case Types.BLOB:
        return "BLOB";
      case Types.CLOB:
        return "CLOB";
      case Types.REF:
        return "REF";
      case Types.DATALINK:
        return "DATALINK";
      case Types.BOOLEAN:
        return "BOOLEAN";
      case Types.ROWID:
        return "ROWID";
      case Types.NCHAR:
        return "NCHAR";
      case Types.NVARCHAR:
        return "NVARCHAR";
      case Types.LONGNVARCHAR:
        return "LONGNVARCHAR";
      case Types.NCLOB:
        return "NCLOB";
      case Types.SQLXML:
        return "SQLXML";
      default:
        return "<UNKNOWN>";
    }
  }
}
