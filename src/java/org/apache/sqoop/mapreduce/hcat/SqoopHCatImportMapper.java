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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.InputJobInfo;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.ImportJobBase;
import org.apache.sqoop.mapreduce.SqoopMapper;

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;

/**
 * A mapper for HCatalog import.
 */
public class SqoopHCatImportMapper extends
  SqoopMapper<LongWritable, SqoopRecord, LongWritable, HCatRecord> {
  public static final Log LOG = LogFactory
    .getLog(SqoopHCatImportMapper.class.getName());
  private static final boolean DEBUG_HCAT_IMPORT_MAPPER =
    Boolean.getBoolean("sqoop.debug.import.mapper");
  private InputJobInfo jobInfo;
  private HCatSchema hCatFullTableSchema;
  private int fieldCount;
  private boolean bigDecimalFormatString;
  private LargeObjectLoader lobLoader;

  @Override
  protected void setup(Context context)
    throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String inputJobInfoStr = conf.get(HCatConstants.HCAT_KEY_JOB_INFO);
    jobInfo =
      (InputJobInfo) HCatUtil.deserialize(inputJobInfoStr);
    HCatSchema tableSchema = jobInfo.getTableInfo().getDataColumns();
    HCatSchema partitionSchema =
      jobInfo.getTableInfo().getPartitionColumns();
    hCatFullTableSchema = new HCatSchema(tableSchema.getFields());
    for (HCatFieldSchema hfs : partitionSchema.getFields()) {
      hCatFullTableSchema.append(hfs);
    }
    fieldCount = hCatFullTableSchema.size();
    lobLoader = new LargeObjectLoader(conf,
      new Path(jobInfo.getTableInfo().getTableLocation()));
    bigDecimalFormatString = conf.getBoolean(
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
  }

  @Override
  public void map(LongWritable key, SqoopRecord value,
    Context context)
    throws IOException, InterruptedException {

    try {
      // Loading of LOBs was delayed until we have a Context.
      value.loadLargeObjects(lobLoader);
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }

    context.write(key, convertToHCatRecord(value));
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }

  private HCatRecord convertToHCatRecord(SqoopRecord sqr)
    throws IOException {
    HCatRecord result = new DefaultHCatRecord(fieldCount);
    Map<String, Object> fieldMap = sqr.getFieldMap();
    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      HCatFieldSchema hfs = hCatFullTableSchema.get(key.toLowerCase());
      if (DEBUG_HCAT_IMPORT_MAPPER) {
        LOG.debug("SqoopRecordVal: field = " + key + " Val " + val
          + " of type " + (val == null ? null : val.getClass().getName())
          + ", hcattype " + hfs.getTypeString());
      }
      result.set(key.toLowerCase(), hCatFullTableSchema,
        toHCat(val, hfs.getType()));
    }
    return result;
  }

  private Object toHCat(Object val, HCatFieldSchema.Type hfsType) {

    if (val == null) {
      return null;
    }

    if (val instanceof Boolean
      || val instanceof Byte
      || val instanceof Short
      || val instanceof Integer
      || val instanceof Long
      || val instanceof String) {
      return val;
    } else if (val instanceof Float) {
      if (hfsType == HCatFieldSchema.Type.DOUBLE) {
        return ((Float) val).doubleValue();
      } else {
        return val;
      }
    } else if (val instanceof Double) {
      if (hfsType == HCatFieldSchema.Type.FLOAT) {
        return ((Double) val).floatValue();
      } else {
        return val;
      }
    } else if (val instanceof BigDecimal) {
      if (bigDecimalFormatString) {
        return ((BigDecimal) val).toPlainString();
      } else {
        return val.toString();
      }
    } else if (val instanceof Date) {
      if (hfsType == Type.STRING) {
        return val;
      } else {
        return ((Date) val).getTime();
      }
    } else if (val instanceof Time) {
      if (hfsType == Type.STRING) {
        return val;
      } else {
        return ((Time) val).getTime();
      }
    } else if (val instanceof Timestamp) {
      if (hfsType == Type.STRING) {
        return val;
      } else {
        return ((Timestamp) val).getTime();
      }
    } else if (val instanceof BytesWritable) {
      BytesWritable bw = (BytesWritable) val;
      return bw.getBytes();
    } else if (val instanceof BlobRef) {
      BlobRef br = (BlobRef) val;
      // Save the ref file for externally store BLOBs as hcatalog bytes, or
      // for inline case, save blob data as hcatalog bytes.
      byte[] bytes = br.isExternal() ? br.toString().getBytes()
        : br.getData();
      return bytes;
    } else if (val instanceof ClobRef) {
      return val;
    }
    throw new UnsupportedOperationException("Objects of type "
      + val.getClass().getName() + " are not suported");

  }

}
