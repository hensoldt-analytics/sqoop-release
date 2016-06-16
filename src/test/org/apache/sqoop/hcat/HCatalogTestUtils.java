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

package org.apache.sqoop.hcat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatSplit;
import org.apache.hive.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.junit.Assert;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;

/**
 * HCatalog common test utilities.
 *
 */
public final class HCatalogTestUtils {
  protected Configuration conf;
  private static List<HCatRecord> recsToLoad = new ArrayList<HCatRecord>();
  private static List<HCatRecord> recsRead = new ArrayList<HCatRecord>();
  private static final Log LOG = LogFactory.getLog(HCatalogTestUtils.class);
  private FileSystem fs;
  private final SqoopHCatUtilities utils = SqoopHCatUtilities.instance();
  private static final double DELTAVAL = 1e-10;
  public static final String SQOOP_HCATALOG_TEST_ARGS =
    "sqoop.hcatalog.test.args";
  private final boolean initialized = false;
  private static String storageInfo = null;
  public static final String STORED_AS_ORCFILE = "stored as\n\torcfile\n";
  public static final String STORED_AS_RCFILE = "stored as\n\trcfile\n";
  public static final String STORED_AS_SEQFILE = "stored as\n\tsequencefile\n";
  public static final String STORED_AS_TEXT = "stored as\n\ttextfile\n";

  // Jackson related
  private ObjectMapper mapper;
  private TypeFactory typeFactory;

  private HCatalogTestUtils() {
    mapper = new ObjectMapper();
    typeFactory = mapper.getTypeFactory();
  }

  private static final class Holder {
    @SuppressWarnings("synthetic-access")
    private static final HCatalogTestUtils INSTANCE = new HCatalogTestUtils();

    private Holder() {
    }
  }

  @SuppressWarnings("synthetic-access")
  public static HCatalogTestUtils instance() {
    return Holder.INSTANCE;
  }

  public static StringBuilder escHCatObj(String objectName) {
    return SqoopHCatUtilities.escHCatObj(objectName);
  }

  public void initUtils() throws IOException, MetaException {
    if (initialized) {
      return;
    }
    conf = new Configuration();
    if (!BaseSqoopTestCase.isOnPhysicalCluster()) {
      conf.set(CommonArgs.FS_DEFAULT_NAME, CommonArgs.LOCAL_FS);
    }
    fs = FileSystem.get(conf);
    fs.initialize(fs.getWorkingDirectory().toUri(), conf);
    storageInfo = null;
    SqoopHCatUtilities.setTestMode(true);
  }

  public static String getStorageInfo() {
    if (null != storageInfo && storageInfo.length() > 0) {
      return storageInfo;
    } else {
      return STORED_AS_RCFILE;
    }
  }

  public void setStorageInfo(String info) {
    storageInfo = info;
  }

  private static String getHCatDropTableCmd(final String dbName,
    final String tableName) {
    return "DROP TABLE IF EXISTS " + escHCatObj(dbName.toLowerCase()) + "."
      + escHCatObj(tableName.toLowerCase());
  }

  private static String getHCatCreateTableCmd(String dbName,
    String tableName, List<HCatFieldSchema> tableCols,
    List<HCatFieldSchema> partKeys) {
    StringBuilder sb = new StringBuilder();
    sb.append("create table ")
      .append(escHCatObj(dbName.toLowerCase()).append('.'));
    sb.append(escHCatObj(tableName.toLowerCase()).append(" (\n\t"));
    for (int i = 0; i < tableCols.size(); ++i) {
      HCatFieldSchema hfs = tableCols.get(i);
      if (i > 0) {
        sb.append(",\n\t");
      }
      sb.append(escHCatObj(hfs.getName().toLowerCase()));
      sb.append(' ').append(hfs.getTypeString());
    }
    sb.append(")\n");
    if (partKeys != null && partKeys.size() > 0) {
      sb.append("partitioned by (\n\t");
      for (int i = 0; i < partKeys.size(); ++i) {
        HCatFieldSchema hfs = partKeys.get(i);
        if (i > 0) {
          sb.append("\n\t,");
        }
        sb.append(escHCatObj(hfs.getName().toLowerCase()));
        sb.append(' ').append(hfs.getTypeString());
      }
      sb.append(")\n");
    }
    sb.append(getStorageInfo());
    LOG.info("Create table command : " + sb);
    return sb.toString();
  }

  /**
   * The record writer mapper for HCatalog tables that writes records from an in
   * memory list.
   */
  public void createHCatTableUsingSchema(String dbName,
    String tableName, List<HCatFieldSchema> tableCols,
    List<HCatFieldSchema> partKeys)
    throws Exception {

    String databaseName = dbName == null
      ? SqoopHCatUtilities.DEFHCATDB : dbName;
    LOG.info("Dropping HCatalog table if it exists " + databaseName
      + '.' + tableName);
    String dropCmd = getHCatDropTableCmd(databaseName, tableName);

    try {
      utils.launchHCatCli(dropCmd);
    } catch (Exception e) {
      LOG.debug("Drop hcatalog table exception : " + e);
      LOG.info("Unable to drop table." + dbName + "."
        + tableName + ".   Assuming it did not exist");
    }
    LOG.info("Creating HCatalog table if it exists " + databaseName
      + '.' + tableName);
    String createCmd = getHCatCreateTableCmd(databaseName, tableName,
      tableCols, partKeys);
    LOG.info("HCatalog create table statement: " + createCmd);
    utils.launchHCatCli(createCmd);
    LOG.info("Created HCatalog table " + dbName + "." + tableName);
  }

  /**
   * The record writer mapper for HCatalog tables that writes records from an in
   * memory list.
   */
  public static class HCatWriterMapper extends
    Mapper<LongWritable, HCatRecord, BytesWritable, HCatRecord> {

    private static int writtenRecordCount = 0;

    public static void setWrittenRecordCount(int count) {
      HCatWriterMapper.writtenRecordCount = count;
    }

    @Override
    public void map(LongWritable key, HCatRecord value,
      Context context)
      throws IOException, InterruptedException {
      try {
        LOG.debug("Writermapper:  Writing record : " + value);
        context.write(null, value);
        writtenRecordCount++;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          e.printStackTrace(System.err);
        }
        throw new IOException(e);
      }
    }
  }

  /**
   * The record reader mapper for HCatalog tables that reads records into an in
   * memory list.
   */
  public static class HCatReaderMapper extends
    Mapper<WritableComparable, HCatRecord, BytesWritable, Text> {

    private static int readRecordCount = 0; // test will be in local mode

    public static void setReadRecordCount(int count) {
      HCatReaderMapper.readRecordCount = count;
    }

    @Override
    public void map(WritableComparable key, HCatRecord value,
      Context context) throws IOException, InterruptedException {
      try {
        recsRead.add(value);
        readRecordCount++;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          e.printStackTrace(System.err);
        }
        throw new IOException(e);
      }
    }
  }

  private void createInputFile(Path path, int rowCount)
    throws IOException {
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    FSDataOutputStream os = fs.create(path);
    for (int i = 0; i < rowCount; i++) {
      String s = i + "\n";
      os.writeChars(s);
    }
    os.close();
  }

  static class HCatRecordInputFormat
          extends InputFormat<LongWritable, HCatRecord> {
    static List<HCatRecord> records;
    static int numRecs;

    public HCatRecordInputFormat() {

    }
    public static void setHCatRecords(List<HCatRecord> recs) {
      records = recs;
      numRecs = records.size();
    }
    public List<InputSplit> getSplits(JobContext context) {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      splits.add(new HCatRecordSplit(records));
      return splits;
    }

    public RecordReader<LongWritable, HCatRecord> createRecordReader
            (InputSplit split, TaskAttemptContext attemptContext)
            throws IOException, InterruptedException {
      return new RecordReader<LongWritable, HCatRecord>() {
        private int curRec;

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
          curRec = -1;
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (curRec >= (numRecs - 1)) {
            return false;
          }
          ++curRec;
          return true;
        }


        public LongWritable getCurrentKey() throws IOException, InterruptedException {
          return new LongWritable((long) curRec);
        }

        public HCatRecord getCurrentValue() throws IOException, InterruptedException {
          return records.get(curRec);
        }

        public float getProgress() throws IOException, InterruptedException {
          return curRec >= (numRecs - 1) ? 1.0f : 0.0f;
        }

        public void close() throws IOException {
          curRec = -1;
        }
      };
    }
  }

  static class HCatRecordSplit extends InputSplit implements Writable {
    List<HCatRecord> records;

    public HCatRecordSplit() {
    }

    public HCatRecordSplit(List<HCatRecord> records) {
       this.records = records;
    }

    @Override
    public String[] getLocations() {
        return new String[] {};
    }

    @Override
    public long getLength() {
      return records.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(records.size());
      for (HCatRecord rec : records) {
        rec.write(out);
        LOG.debug("HCat record being serialized : " + rec);

      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      this.records = new ArrayList<HCatRecord>(size);
      for (int i = 0; i < size; ++i) {
        HCatRecord rec = new DefaultHCatRecord();
        rec.readFields(in);
        LOG.debug("HCat record after reading deserialization : " + rec);
      }
    }
  }


  public List<HCatRecord> loadHCatTable(String dbName,
    String tableName, HCatSchema tblSchema, Map<String, String> partKeyMap,
    List<HCatRecord> records)
    throws Exception {

    Job job = new Job(conf, "HCat load job");

    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatWriterMapper.class);

    job.getConfiguration()
      .setInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, 1);;

    HCatWriterMapper.setWrittenRecordCount(0);

    job.setInputFormatClass(HCatRecordInputFormat.class);
    HCatRecordInputFormat.setHCatRecords(records);
    job.setOutputFormatClass(HCatOutputFormat.class);
    OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tableName,
      partKeyMap);

    HCatOutputFormat.setOutput(job, outputJobInfo);

    HCatOutputFormat.setSchema(job, tblSchema);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(HCatRecord.class);

    job.setNumReduceTasks(0);
    SqoopHCatUtilities.addJars(job, new SqoopOptions());
    boolean success = job.waitForCompletion(true);

    if (!success) {
      throw new IOException("Loading HCatalog table with test records failed");
    }
    utils.invokeOutputCommitterForLocalMode(job);
    LOG.info("Loaded " + HCatWriterMapper.writtenRecordCount + " records");
    return recsToLoad;
  }

  /**
   * Run a local map reduce job to read records from HCatalog table.
   * @return
   * @throws Exception
   */
  public List<HCatRecord> readHCatRecords(String dbName,
    String tableName, String filter) throws Exception {

    HCatReaderMapper.setReadRecordCount(0);
    recsRead.clear();

    // Configuration conf = new Configuration();
    Job job = new Job(conf, "HCatalog reader job");
    job.setJarByClass(this.getClass());
    job.setMapperClass(HCatReaderMapper.class);
    job.getConfiguration()
      .setInt(ConfigurationConstants.PROP_MAPRED_MAP_TASKS, 1);
    // input/output settings
    job.setInputFormatClass(HCatInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    HCatInputFormat.setInput(job, dbName, tableName).setFilter(filter);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    Path path = new Path(fs.getWorkingDirectory(),
      "mapreduce/HCatTableIndexOutput");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    FileOutputFormat.setOutputPath(job, path);

    job.waitForCompletion(true);
    LOG.info("Read " + HCatReaderMapper.readRecordCount + " records");

    return recsRead;
  }

  /**
   * An enumeration type to hold the partition key type of the ColumnGenerator
   * defined columns.
   */
  public enum KeyType {
    NOT_A_KEY,
    STATIC_KEY,
    DYNAMIC_KEY
  };

  /**
   * An enumeration type to hold the creation mode of the HCatalog table.
   */
  public enum CreateMode {
    NO_CREATION,
    CREATE,
    CREATE_AND_LOAD,
  };

  /**
   * When generating data for export tests, each column is generated according
   * to a ColumnGenerator.
   */
   public static class ColumnGenerator {

    private String name;
    private String dbType;
    private int sqlType;
    private HCatFieldSchema.Type hCatType;
    private int hCatPrecision;
    private int hCatScale;
    private Object hCatValue;
    private Object dbValue;
    private KeyType keyType;
    private String hCatTypeName;

    public ColumnGenerator(final String name,
                           final String dbType, final int sqlType,
                           final HCatFieldSchema.Type hCatType, final int hCatPrecision,
                           final int hCatScale, final Object hCatValue,
                           final Object dbValue, final KeyType keyType) {
      this.name = name;
      this.dbType = dbType;
      this.sqlType = sqlType;
      this.hCatType = hCatType;
      this.hCatPrecision =  hCatPrecision;
      this.hCatScale = hCatScale;
      this.hCatValue = hCatValue;
      this.dbValue = dbValue;
      this.keyType = keyType;
    }

    public ColumnGenerator(final String name,
                           final String dbType, final int sqlType,
                           final HCatFieldSchema.Type hCatType, final int hCatPrecision,
                           final int hCatScale, final Object hCatValue,
                           final Object dbValue, final KeyType keyType,
                           final String hCatTypeName) {
      this(name, dbType, sqlType, hCatType, hCatPrecision, hCatScale, hCatValue, dbValue, keyType);
      this.hCatTypeName = hCatTypeName;
    }
    /*
     * The column name
     */
    public String getName() { return name; }

    /**
     * For a row with id rowNum, what should we write into that HCatalog column
     * to export?
     */
    public Object getHCatValue(int rowNum) { return hCatValue; }

    /**
     * For a row with id rowNum, what should the database return for the given
     * column's value?
     */
    public Object getDBValue(int rowNum) { return dbValue; }

    /** Return the column type to put in the CREATE TABLE statement. */
    public String getDBTypeString() { return  dbType;}

    /** Return the SqlType for this column. */
    public int getSqlType() { return sqlType; }

    /** Return the HCat type for this column. */
    HCatFieldSchema.Type getHCatType() { return hCatType; }

    /** Return the precision/length of the field if any. */
    public int getHCatPrecision() {return hCatPrecision;}

    /** Return the scale of the field if any. */
    public int getHCatScale() { return hCatScale; }

    /**
     * If the field is a partition key, then whether is part of the static
     * partitioning specification in imports or exports. Only one key can be a
     * static partitioning key. After the first column marked as static, rest of
     * the keys will be considered dynamic even if they are marked static.
     */
    public KeyType getKeyType() { return keyType;}

    public String hCatTypeName() {
      return hCatTypeName;
    }
  }


  /**
   * Return the column name for a column index. Each table contains two columns
   * named 'id' and 'msg', and then an arbitrary number of additional columns
   * defined by ColumnGenerators. These columns are referenced by idx 0, 1, 2
   * and on.
   * @param idx
   *          the index of the ColumnGenerator in the array passed to
   *          createTable().
   * @return the name of the column
   */
  public static String forIdx(int idx) {
    return "COL" + idx;
  }

  public static ColumnGenerator colGenerator(final String name,
    final String dbType, final int sqlType,
    final HCatFieldSchema.Type hCatType, final int hCatPrecision,
    final int hCatScale, final Object hCatValue,
    final Object dbValue, final KeyType keyType) {
    ColumnGenerator gen = new ColumnGenerator(name, dbType, sqlType, hCatType,
            hCatPrecision, hCatScale, hCatValue, dbValue, keyType);
    return gen;
  }
  public static ColumnGenerator colGeneratorWithHCatType(final String name,
                                             final String dbType, final int sqlType,
                                             final HCatFieldSchema.Type hCatType, final int hCatPrecision,
                                             final int hCatScale, final Object hCatValue,
                                             final Object dbValue, final KeyType keyType,
                                             final String hCatTypeString) {
    ColumnGenerator gen = new ColumnGenerator(name, dbType, sqlType, hCatType,
            hCatPrecision, hCatScale, hCatValue, dbValue, keyType, hCatTypeString);
    return gen;
  }

  public static void assertEquals(Object expectedVal,
    Object actualVal) {

    if (expectedVal != null && expectedVal instanceof byte[]) {
      Assert
        .assertArrayEquals((byte[]) expectedVal, (byte[]) actualVal);
    } else {
      if (expectedVal instanceof Float) {
        if (actualVal instanceof Double) {
          Assert.assertEquals(((Float) expectedVal).floatValue(),
                  ((Double) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
                  .assertEquals("Got unexpected column value", expectedVal,
                          actualVal);
        }
      } else if (expectedVal instanceof Double) {
        if (actualVal instanceof Float) {
          Assert.assertEquals(((Double) expectedVal).doubleValue(),
                  ((Float) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
                  .assertEquals("Got unexpected column value", expectedVal,
                          actualVal);
        }
      } else if (expectedVal instanceof HiveVarchar) {
        HiveVarchar vc1 = (HiveVarchar) expectedVal;
        if (actualVal instanceof HiveVarchar) {
          HiveVarchar vc2 = (HiveVarchar) actualVal;
          Assert.assertEquals(vc1.getCharacterLength(), vc2.getCharacterLength());
          Assert.assertEquals(vc1.getValue(), vc2.getValue());
        } else {
          String vc2 = (String) actualVal;
          Assert.assertEquals(vc1.getCharacterLength(), vc2.length());
          Assert.assertEquals(vc1.getValue(), vc2);
        }
      } else if (expectedVal instanceof HiveChar) {
        HiveChar c1 = (HiveChar) expectedVal;
        if (actualVal instanceof HiveChar) {
          HiveChar c2 = (HiveChar) actualVal;
          Assert.assertEquals(c1.getCharacterLength(), c2.getCharacterLength());
          Assert.assertEquals(c1.getValue(), c2.getValue());
        } else {
          String c2 = (String) actualVal;
          Assert.assertEquals(c1.getCharacterLength(), c2.length());
          Assert.assertEquals(c1.getValue(), c2);
        }
      } else if (expectedVal instanceof Map && actualVal instanceof Map) {
        // While we can use Map.equals, the following will help us pinpoint the
        // failure
        Map expectedMap = (Map) expectedVal;
        Map actualMap = (Map) actualVal;

        Set<Map.Entry> expectedSet = expectedMap.keySet();
        Set<Map.Entry> actualSet = actualMap.keySet();
        Assert.assertEquals("Expected Map key size  not equal to actual map",
                  expectedSet.size(), actualSet.size());

        for (Object o : expectedSet) {
          if (actualMap.get(o) == null) {
            LOG.debug("Expected Map key " + o + " not in actual value");
          }
          LOG.debug("Comparing values for Map key does not match");
          assertEquals(expectedMap.get(o), actualMap.get(o));
        }
      } else if (expectedVal instanceof List && actualVal instanceof List) {
        List expectedList = (List) expectedVal;
        List actualList = (List) actualVal;
        Assert.assertEquals("Expected List size  not equal to actual list",
                  expectedList.size(), actualList.size());

        for (int i = 0; i < expectedList.size(); ++i) {
          LOG.debug("Comparing list element at index: " + i);
          assertEquals(expectedList.get(i), actualList.get(i));
        }
      } else {
        Assert.assertEquals(expectedVal, actualVal);
      }
    }
  }

  /**
   * Verify that on a given row, a column has a given value.
   *
   * @param id
   *          the id column specifying the row to test.
   */
  public void assertSqlColValForRowId(Connection conn,
    String table, int id, String colName,
    Object expectedVal) throws SQLException {
    LOG.info("Verifying column " + colName + " has value " + expectedVal);

    PreparedStatement statement = conn.prepareStatement(
      "SELECT " + colName + " FROM " + table + " WHERE id = " + id,
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    Object actualVal = null;
    try {
      ResultSet rs = statement.executeQuery();
      try {
        rs.next();
        actualVal = rs.getObject(1);
      } finally {
        rs.close();
      }
    } finally {
      statement.close();
    }

    assertEquals(expectedVal, actualVal);
  }

  /**
   * Verify that on a given row, a column has a given value.
   *
   * @param id
   *          the id column specifying the row to test.
   */
  public static void assertHCatColValForRowId(List<HCatRecord> recs,
    HCatSchema schema, int id, String fieldName,
    Object expectedVal) throws IOException {
    LOG.info("Verifying field " + fieldName + " has value " + expectedVal);

    Object actualVal = null;
    for (HCatRecord rec : recs) {
      if (rec.getInteger("id", schema).equals(id)) {
        actualVal = rec.get(fieldName, schema);
        break;
      }
    }
    if (actualVal == null) {
      throw new IOException("No record found with id = " + id);
    }
    if (expectedVal != null && expectedVal instanceof byte[]) {
      Assert
        .assertArrayEquals((byte[]) expectedVal, (byte[]) actualVal);
    } else {
      if (expectedVal instanceof Float) {
        if (actualVal instanceof Double) {
          Assert.assertEquals(((Float) expectedVal).floatValue(),
            ((Double) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
            .assertEquals("Got unexpected column value", expectedVal,
              actualVal);
        }
      } else if (expectedVal instanceof Double) {
        if (actualVal instanceof Float) {
          Assert.assertEquals(((Double) expectedVal).doubleValue(),
            ((Float) actualVal).doubleValue(), DELTAVAL);
        } else {
          Assert
            .assertEquals("Got unexpected column value", expectedVal,
              actualVal);
        }
      } else {
        Assert
          .assertEquals("Got unexpected column value", expectedVal,
            actualVal);
      }
    }
  }

  /**
   * Return a SQL statement that drops a table, if it exists.
   *
   * @param tableName
   *          the table to drop.
   * @return the SQL statement to drop that table.
   */
  public static String getSqlDropTableStatement(String tableName) {
    return "DROP TABLE " + tableName;
  }

  public static String getSqlCreateTableStatement(String tableName,
    ColumnGenerator... extraCols) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(tableName);
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraCols) {
      sb.append(", \"" + gen.getName() + "\" " + gen.getDBTypeString());
    }
    sb.append(")");
    String cmd = sb.toString();
    LOG.debug("Generated SQL create table command : " + cmd);
    return cmd;
  }

  public static String getSqlInsertTableStatement(String tableName,
    ColumnGenerator... extraCols) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    sb.append(tableName);
    sb.append(" (ID, MSG");
    for (int i = 0; i < extraCols.length; ++i) {
      sb.append(", \"").append(extraCols[i].getName()).append('"');
    }
    sb.append(") VALUES ( ?, ?");
    for (int i = 0; i < extraCols.length; ++i) {
      sb.append(", ?");
    }
    sb.append(")");
    String s = sb.toString();
    LOG.debug("Generated SQL insert table command : " + s);
    return s;
  }

  public void createSqlTable(Connection conn, boolean generateOnly,
    int count, String table, ColumnGenerator... extraCols)
    throws Exception {
    PreparedStatement statement = conn.prepareStatement(
      getSqlDropTableStatement(table),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } catch (SQLException sqle) {
      conn.rollback();
    } finally {
      statement.close();
    }
    statement = conn.prepareStatement(
      getSqlCreateTableStatement(table, extraCols),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      statement.executeUpdate();
      conn.commit();
    } finally {
      statement.close();
    }
    if (!generateOnly) {
      loadSqlTable(conn, table, count, extraCols);
    }
  }

  public HCatSchema createHCatTable(CreateMode mode, int count,
    String table, ColumnGenerator... extraCols)
    throws Exception {
    HCatSchema hCatTblSchema = generateHCatTableSchema(extraCols);
    HCatSchema hCatPartSchema = generateHCatPartitionSchema(extraCols);
    HCatSchema hCatFullSchema = new HCatSchema(hCatTblSchema.getFields());
    for (HCatFieldSchema hfs : hCatPartSchema.getFields()) {
      hCatFullSchema.append(hfs);
    }
    if (mode != CreateMode.NO_CREATION) {

      createHCatTableUsingSchema(null, table,
        hCatTblSchema.getFields(), hCatPartSchema.getFields());
      if (mode == CreateMode.CREATE_AND_LOAD) {
        HCatSchema hCatLoadSchema = new HCatSchema(hCatTblSchema.getFields());
        HCatSchema dynPartSchema =
          generateHCatDynamicPartitionSchema(extraCols);
        for (HCatFieldSchema hfs : dynPartSchema.getFields()) {
          hCatLoadSchema.append(hfs);
        }
        loadHCatTable(hCatLoadSchema, table, count, extraCols);
      }
    }
    return hCatFullSchema;
  }

  private void loadHCatTable(HCatSchema hCatSchema, String table,
    int count, ColumnGenerator... extraCols)
    throws Exception {
    Map<String, String> staticKeyMap = new HashMap<String, String>();
    for (ColumnGenerator col : extraCols) {
      if (col.getKeyType() == KeyType.STATIC_KEY) {
        staticKeyMap.put(col.getName(), (String) col.getHCatValue(0));
      }
    }
    loadHCatTable(null, table, hCatSchema, staticKeyMap,
      generateHCatRecords(count, hCatSchema, extraCols));
  }

  private void loadSqlTable(Connection conn, String table, int count,
    ColumnGenerator... extraCols) throws Exception {
    PreparedStatement statement = conn.prepareStatement(
      getSqlInsertTableStatement(table, extraCols),
      ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    try {
      for (int i = 0; i < count; ++i) {
        statement.setObject(1, i, Types.INTEGER);
        statement.setObject(2, "textfield" + i, Types.VARCHAR);
        for (int j = 0; j < extraCols.length; ++j) {
          statement.setObject(j + 3, extraCols[j].getDBValue(i),
            extraCols[j].getSqlType());
        }
        statement.executeUpdate();
      }
      if (!conn.getAutoCommit()) {
        conn.commit();
      }
    } finally {
      statement.close();
    }
  }

  private HCatSchema generateHCatTableSchema(ColumnGenerator... extraCols)
    throws Exception {
    List<HCatFieldSchema> hCatTblCols = new ArrayList<HCatFieldSchema>();
    hCatTblCols.clear();
    PrimitiveTypeInfo ptInfo;
    TypeInfo tInfo;
    ptInfo = new PrimitiveTypeInfo();
    ptInfo.setTypeName(HCatFieldSchema.Type.INT.name().toLowerCase());
    hCatTblCols.add(new HCatFieldSchema("id", ptInfo, ""));
    ptInfo = new PrimitiveTypeInfo();
    ptInfo.setTypeName(HCatFieldSchema.Type.STRING.name().toLowerCase());
    hCatTblCols
      .add(new HCatFieldSchema("msg", ptInfo, ""));
    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() == KeyType.NOT_A_KEY) {
        switch(gen.getHCatType()) {
          case CHAR:
            ptInfo = new CharTypeInfo(gen.getHCatPrecision());
            hCatTblCols
                    .add(new HCatFieldSchema(gen.getName().toLowerCase(), ptInfo, ""));
            break;
          case VARCHAR:
            ptInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            hCatTblCols
                    .add(new HCatFieldSchema(gen.getName().toLowerCase(), ptInfo, ""));
            break;
          case DECIMAL:
            ptInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
              gen.getHCatScale());
            hCatTblCols
                    .add(new HCatFieldSchema(gen.getName().toLowerCase(), ptInfo, ""));
            break;
          case ARRAY:
            List<TypeInfo> colTypes = TypeInfoUtils
                    .getTypeInfosFromTypeString(gen.hCatTypeName());
            ListTypeInfo listTypeInfo = (ListTypeInfo) colTypes.get(0);
            hCatTblCols.add(new HCatFieldSchema(gen.getName(), gen.getHCatType(),
                    HCatSchemaUtils.getHCatSchema(listTypeInfo.getListElementTypeInfo()), ""));
            break;
          case STRUCT:
            List<TypeInfo> colTypes2 = TypeInfoUtils
                    .getTypeInfosFromTypeString(gen.hCatTypeName());
            StructTypeInfo structSchema = (StructTypeInfo) colTypes2.get(0);
            hCatTblCols.add(new HCatFieldSchema(gen.getName(), gen.getHCatType(),
                    HCatSchemaUtils.getHCatSchema(structSchema), ""));
            break;
          case MAP:
            colTypes = TypeInfoUtils.getTypeInfosFromTypeString(gen.hCatTypeName());
            MapTypeInfo mapTypeInfo = (MapTypeInfo)colTypes.get(0);
            HCatSchema valueSchema = HCatSchemaUtils.getHCatSchema(mapTypeInfo.getMapValueTypeInfo());
            hCatTblCols.add(HCatFieldSchema.createMapTypeFieldSchema(gen.getName(),
                    (PrimitiveTypeInfo)mapTypeInfo.getMapKeyTypeInfo(), valueSchema, ""));
            break;
          default:
            ptInfo = new PrimitiveTypeInfo();
            ptInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            hCatTblCols
                    .add(new HCatFieldSchema(gen.getName().toLowerCase(), ptInfo, ""));
            break;
        }

      }
    }
    HCatSchema hCatTblSchema = new HCatSchema(hCatTblCols);
    LOG.debug("Generated table schema\n\t" + hCatTblSchema);
    return hCatTblSchema;
  }

  private HCatSchema generateHCatPartitionSchema(ColumnGenerator... extraCols)
    throws Exception {
    List<HCatFieldSchema> hCatPartCols = new ArrayList<HCatFieldSchema>();
    PrimitiveTypeInfo tInfo;

    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() != KeyType.NOT_A_KEY) {
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
            gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatPartCols
          .add(new HCatFieldSchema(gen.getName().toLowerCase(), tInfo, ""));
      }
    }
    HCatSchema hCatPartSchema = new HCatSchema(hCatPartCols);
    return hCatPartSchema;
  }

  private HCatSchema generateHCatDynamicPartitionSchema(
    ColumnGenerator... extraCols) throws Exception {
    List<HCatFieldSchema> hCatPartCols = new ArrayList<HCatFieldSchema>();
    PrimitiveTypeInfo tInfo;

    hCatPartCols.clear();
    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() != KeyType.NOT_A_KEY) {
        if (gen.getKeyType() == KeyType.STATIC_KEY) {
          continue;
        }
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
            gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatPartCols
          .add(new HCatFieldSchema(gen.getName().toLowerCase(), tInfo, ""));
      }
    }
    HCatSchema hCatPartSchema = new HCatSchema(hCatPartCols);
    return hCatPartSchema;

  }

  private HCatSchema generateHCatStaticPartitionSchema(
    ColumnGenerator... extraCols) throws Exception {
    List<HCatFieldSchema> hCatPartCols = new ArrayList<HCatFieldSchema>();
    PrimitiveTypeInfo tInfo;

    hCatPartCols.clear();
    for (ColumnGenerator gen : extraCols) {
      if (gen.getKeyType() == KeyType.STATIC_KEY) {
        switch(gen.getHCatType()) {
          case CHAR:
            tInfo = new CharTypeInfo(gen.getHCatPrecision());
            break;
          case VARCHAR:
            tInfo = new VarcharTypeInfo(gen.getHCatPrecision());
            break;
          case DECIMAL:
            tInfo = new DecimalTypeInfo(gen.getHCatPrecision(),
            gen.getHCatScale());
            break;
          default:
            tInfo = new PrimitiveTypeInfo();
            tInfo.setTypeName(gen.getHCatType().name().toLowerCase());
            break;
        }
        hCatPartCols
          .add(new HCatFieldSchema(gen.getName(), tInfo, ""));
        break;
      }
    }
    HCatSchema hCatPartSchema = new HCatSchema(hCatPartCols);
    return hCatPartSchema;
  }

  private List<HCatRecord> generateHCatRecords(int numRecords,
    HCatSchema hCatTblSchema, ColumnGenerator... extraCols) throws Exception {
    List<HCatRecord> records = new ArrayList<HCatRecord>();
    List<HCatFieldSchema> hCatTblCols = hCatTblSchema.getFields();
    int size = hCatTblCols.size();
    for (int i = 0; i < numRecords; ++i) {
      DefaultHCatRecord record = new DefaultHCatRecord(size);
      record.set(hCatTblCols.get(0).getName(), hCatTblSchema, i);
      record.set(hCatTblCols.get(1).getName(), hCatTblSchema, "textfield" + i);
      int idx = 0;
      for (int j = 0; j < extraCols.length; ++j) {
        if (extraCols[j].getKeyType() == KeyType.STATIC_KEY) {
          continue;
        }
        record.set(hCatTblCols.get(idx + 2).getName(), hCatTblSchema,
          extraCols[j].getHCatValue(i));
        ++idx;
      }

      records.add(record);
    }
    LOG.debug("Dump of generated records :\n\n" + hCatRecordDump(records, hCatTblSchema));
    return records;
  }

  public String hCatRecordDump(List<HCatRecord> recs,
    HCatSchema schema) throws Exception {
    List<String> fields = schema.getFieldNames();
    int count = 0;
    StringBuilder sb = new StringBuilder(1024);
    for (HCatRecord rec : recs) {
      sb.append("HCat Record : " + ++count).append('\n');
      for (String field : fields) {
        sb.append('\t').append(field).append('=');
        sb.append(rec.get(field, schema)).append('\n');
        sb.append("\n\n");
      }
    }
    return sb.toString();
  }

  public Map<String, String> getAddlTestArgs() {
    String addlArgs = System.getProperty(SQOOP_HCATALOG_TEST_ARGS);
    Map<String, String> addlArgsMap = new HashMap<String, String>();
    if (addlArgs != null) {
      String[] argsArray = addlArgs.split(",");
      for (String s : argsArray) {
        String[] keyVal = s.split("=");
        if (keyVal.length == 2) {
          addlArgsMap.put(keyVal[0], keyVal[1]);
        } else {
          LOG.info("Ignoring malformed addl arg " + s);
        }
      }
    }
    return addlArgsMap;
  }
}
