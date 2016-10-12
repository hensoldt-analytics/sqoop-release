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

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.StorerInfo;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.ImportJobBase;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.ArrayType;
import org.codehaus.jackson.map.type.CollectionType;
import org.codehaus.jackson.map.type.MapType;
import org.codehaus.jackson.map.type.TypeFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for Sqoop HCat Integration import jobs.
 */
public class SqoopHCatImportHelper {
  public static final Log LOG = LogFactory.getLog(SqoopHCatImportHelper.class
    .getName());

  private static boolean debugHCatImportMapper = false;

  private InputJobInfo jobInfo;
  private HCatSchema hCatFullTableSchema;
  private int fieldCount;
  private boolean bigDecimalFormatString;
  private LargeObjectLoader lobLoader;
  private HCatSchema partitionSchema = null;
  private HCatSchema dataColsSchema = null;
  private String hiveDelimsReplacement;
  private boolean doHiveDelimsReplacement = false;
  private DelimiterSet hiveDelimiters;
  private String[] staticPartitionKeys;
  private int[] hCatFieldPositions;
  private int colCount;
  private Pattern numberPattern;
  // Jackson related
  private ObjectMapper mapper;


  public SqoopHCatImportHelper(Configuration conf) throws IOException,
    InterruptedException {

    String inputJobInfoStr = conf.get(HCatConstants.HCAT_KEY_JOB_INFO);
    jobInfo = (InputJobInfo) HCatUtil.deserialize(inputJobInfoStr);
    dataColsSchema = jobInfo.getTableInfo().getDataColumns();
    partitionSchema = jobInfo.getTableInfo().getPartitionColumns();
    StringBuilder storerInfoStr = new StringBuilder(1024);
    StorerInfo storerInfo = jobInfo.getTableInfo().getStorerInfo();
    storerInfoStr.append("HCatalog Storer Info : ").append("\n\tHandler = ")
      .append(storerInfo.getStorageHandlerClass())
      .append("\n\tInput format class = ").append(storerInfo.getIfClass())
      .append("\n\tOutput format class = ").append(storerInfo.getOfClass())
      .append("\n\tSerde class = ").append(storerInfo.getSerdeClass());
    Properties storerProperties = storerInfo.getProperties();
    if (!storerProperties.isEmpty()) {
      storerInfoStr.append("\nStorer properties ");
      for (Map.Entry<Object, Object> entry : storerProperties.entrySet()) {
        String key = (String) entry.getKey();
        Object val = entry.getValue();
        storerInfoStr.append("\n\t").append(key).append('=').append(val);
      }
    }
    storerInfoStr.append("\n");
    LOG.info(storerInfoStr);

    hCatFullTableSchema = new HCatSchema(dataColsSchema.getFields());
    for (HCatFieldSchema hfs : partitionSchema.getFields()) {
      hCatFullTableSchema.append(hfs);
    }
    fieldCount = hCatFullTableSchema.size();
    lobLoader = new LargeObjectLoader(conf, new Path(jobInfo.getTableInfo()
      .getTableLocation()));
    bigDecimalFormatString = conf.getBoolean(
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    debugHCatImportMapper = conf.getBoolean(
      SqoopHCatUtilities.DEBUG_HCAT_IMPORT_MAPPER_PROP, false);
    IntWritable[] delimChars = DefaultStringifier.loadArray(conf,
      SqoopHCatUtilities.HIVE_DELIMITERS_TO_REPLACE_PROP, IntWritable.class);
    hiveDelimiters = new DelimiterSet((char) delimChars[0].get(),
      (char) delimChars[1].get(), (char) delimChars[2].get(),
      (char) delimChars[3].get(), delimChars[4].get() == 1 ? true : false);
    hiveDelimsReplacement = conf
      .get(SqoopHCatUtilities.HIVE_DELIMITERS_REPLACEMENT_PROP);
    if (hiveDelimsReplacement == null) {
      hiveDelimsReplacement = "";
    }
    doHiveDelimsReplacement = Boolean.valueOf(conf
      .get(SqoopHCatUtilities.HIVE_DELIMITERS_REPLACEMENT_ENABLED_PROP));

    IntWritable[] fPos = DefaultStringifier.loadArray(conf,
      SqoopHCatUtilities.HCAT_FIELD_POSITIONS_PROP, IntWritable.class);
    hCatFieldPositions = new int[fPos.length];
    for (int i = 0; i < fPos.length; ++i) {
      hCatFieldPositions[i] = fPos[i].get();
    }
    numberPattern = Pattern.compile("\\d+");
    mapper = new ObjectMapper();


    LOG.debug("Hive delims replacement enabled : " + doHiveDelimsReplacement);
    LOG.debug("Hive Delimiters : " + hiveDelimiters.toString());
    LOG.debug("Hive delimiters replacement : " + hiveDelimsReplacement);
    staticPartitionKeys = conf
      .getStrings(SqoopHCatUtilities.HCAT_STATIC_PARTITION_KEY_PROP);
    String partKeysString = staticPartitionKeys == null ? ""
      : Arrays.toString(staticPartitionKeys);
    LOG.debug("Static partition key used : "  + partKeysString);
  }

  public HCatRecord convertToHCatRecord(SqoopRecord sqr) throws IOException,
    InterruptedException {
    try {
      // Loading of LOBs was delayed until we have a Context.
      sqr.loadLargeObjects(lobLoader);
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }
    if (colCount == -1) {
      colCount = sqr.getFieldMap().size();
    }

    Map<String, Object> fieldMap = sqr.getFieldMap();
    HCatRecord result = new DefaultHCatRecord(fieldCount);

    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      String hfn = key.toLowerCase();
      boolean skip = false;
      if (staticPartitionKeys != null && staticPartitionKeys.length > 0) {
        for (int i = 0; i < staticPartitionKeys.length; ++i) {
          if (staticPartitionKeys[i].equals(hfn)) {
            skip = true;
            break;
          }
        }
      }
      if (skip) {
        continue;
      }
      HCatFieldSchema hfs = null;
      try {
        hfs = hCatFullTableSchema.get(hfn);
      } catch (Exception e) {
        throw new IOException("Unable to lookup " + hfn + " in the hcat schema");
      }
      if (debugHCatImportMapper) {
        LOG.debug("SqoopRecordVal: field = " + key + " Val " + val
          + " of type " + (val == null ? null : val.getClass().getName())
          + ", hcattype " + hfs.getTypeString());
      }
      Object hCatVal = toHCat(val, hfs);

      result.set(hfn, hCatFullTableSchema, hCatVal);
    }

    return result;
  }

  private Object toHCat(Object val, HCatFieldSchema hfs) throws IOException {
    HCatFieldSchema.Type hfsType = hfs.getType();
    if (val == null) {
      return null;
    }

    Object retVal = null;
    if (hfs.isComplex()) {
      String str;
      Map<?, ?> map = null;
      List<?> struct = null;
      List<?> array = null;
      if (val instanceof Map) {
        map = convertJSonMapToHCatMap((Map<?,?>) val, hfs);
        return map;
      } else if (val instanceof List){
        if (hfs.getType() == HCatFieldSchema.Type.ARRAY) {
           array = convertJSonListToHCatArray((List<?>) val, hfs);
          return array;
        } else {
          struct = convertJSonListToHCatStruct((List<?>) val, hfs);
          return struct;
        }
      } else {
        if (val instanceof String) {
          str = val.toString();
        } else if (val instanceof ClobRef) {
          ClobRef cr = (ClobRef) val;
          str = cr.isExternal() ? cr.toString() : cr.getData();
        }  else {
          return null;
        }
        switch (hfs.getType()) {
          case ARRAY:
            array = convertJSonStringToHCatArray(str, hfs);
            return array;
          case MAP:
            map = convertJSonStringToHCatMap(str, hfs);
            return map;
          case STRUCT:
            struct = convertJSonStringToHCatStruct(str, hfs);
            return struct;
        }
      }
    }
    else {
      PrimitiveTypeInfo typeInfo = hfs.getTypeInfo();
      if (val instanceof Number) {
        retVal = convertNumberToPrimitiveTypes(val, typeInfo);
      } else if (val instanceof Boolean) {
        retVal = convertBooleanToPrimitiveTypes(val, typeInfo);
      } else if (val instanceof String) {
        retVal = convertStringToPrimitiveTypes(val.toString(), typeInfo);
      } else if (val instanceof java.util.Date) {
        retVal = converDateToPrimitiveTypes(val, typeInfo);
      } else if (val instanceof BytesWritable) {
        if (hfsType == HCatFieldSchema.Type.BINARY) {
          BytesWritable bw = (BytesWritable) val;
          retVal = bw.getBytes();
        }
      } else if (val instanceof BlobRef) {
        if (hfsType == HCatFieldSchema.Type.BINARY) {
          BlobRef br = (BlobRef) val;
          byte[] bytes = br.isExternal() ? br.toString().getBytes() : br.getData();
          retVal = bytes;
        }
      } else if (val instanceof ClobRef) {
        retVal = convertClobToPrimitiveTypes(val, typeInfo);
      } else {
        throw new UnsupportedOperationException("Objects of type "
                + val.getClass().getName() + " are not suported");
      }
      if (retVal == null) {
        LOG.error("Unable to convert [" + val
                + "]  of type " + val.getClass().getName()
                + " to HCatalog type " + hfs.getTypeString());
      }
    }
    return retVal;
  }

  private Object convertClobToPrimitiveTypes(Object val, PrimitiveTypeInfo typeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    ClobRef cr = (ClobRef) val;
    String s = cr.isExternal() ? cr.toString() : cr.getData();
    switch(category) {
      case STRING:
        return s;
      case VARCHAR:
        VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
        HiveVarchar hvc = new HiveVarchar(s, vti.getLength());
        return hvc;
      case CHAR:
        CharTypeInfo cti = (CharTypeInfo) typeInfo;
        HiveChar hc = new HiveChar(s, cti.getLength());
        return hc;
    }
    return null;
  }

  private Object converDateToPrimitiveTypes(Object val, PrimitiveTypeInfo typeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    Date d;
    Time t;
    Timestamp ts;
    if (val instanceof java.sql.Date) {
      d = (Date) val;
      switch(category) {
        case DATE:
          return d;
        case TIMESTAMP:
          return new Timestamp(d.getTime());
        case LONG:
          return (d.getTime());
        case STRING:
          return val.toString();
        case VARCHAR:
          VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
          HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
          return hvc;
        case CHAR:
          CharTypeInfo cti = (CharTypeInfo) typeInfo;
          HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
          return hChar;
      }
    } else if (val instanceof java.sql.Time) {
      t = (Time) val;
      switch(category) {
        case DATE:
          return new Date(t.getTime());
        case TIMESTAMP:
          return new Timestamp(t.getTime());
        case LONG:
          return ((Time) val).getTime();
        case STRING:
          return val.toString();
        case VARCHAR:
          VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
          HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
          return hvc;
        case CHAR:
          CharTypeInfo cti = (CharTypeInfo) typeInfo;
          HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
          return hChar;
      }
    } else if (val instanceof java.sql.Timestamp) {
      ts = (Timestamp) val;
      switch(category) {
        case DATE:
          return new Date(ts.getTime());
        case TIMESTAMP:
          return ts;
        case LONG:
          return ts.getTime();
        case STRING:
          return val.toString();
        case VARCHAR:
          VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
          HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
          return hvc;
        case CHAR:
          CharTypeInfo cti = (CharTypeInfo) typeInfo;
          HiveChar hc = new HiveChar(val.toString(), cti.getLength());
          return hc;
      }
    }
    return null;
  }

  private Object convertStringToPrimitiveTypes(String str, PrimitiveTypeInfo typeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    switch (category) {
      case STRING:
      case VARCHAR:
      case CHAR:
        if (doHiveDelimsReplacement) {
          str = FieldFormatter.hiveStringReplaceDelims(str,
                  hiveDelimsReplacement, hiveDelimiters);
        }
        if (category == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
          return str;
        } else if (category == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) {
          VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
          HiveVarchar hvc = new HiveVarchar(str, vti.getLength());
          return hvc;
        } else if (category == PrimitiveObjectInspector.PrimitiveCategory.CHAR) {
          CharTypeInfo cti = (CharTypeInfo) typeInfo;
          HiveChar hc = new HiveChar(str, cti.getLength());
          return hc;
        }
        break;
      case DECIMAL:
        DecimalTypeInfo dti = (DecimalTypeInfo) typeInfo;
        BigDecimal bd = new BigDecimal(str, new MathContext(dti.precision(), RoundingMode.HALF_UP));
        bd.setScale(dti.scale(), BigDecimal.ROUND_HALF_UP);
        return HiveDecimal.create(bd);
      case BYTE:
        Byte b = Byte.valueOf(str);
        return b;
      case SHORT:
        Short s = Short.valueOf(str);
        return s;
      case INT:
        Integer i = Integer.valueOf(str);
        return i;
      case LONG:
        Long l = Long.valueOf(str);
        return l;
      case FLOAT:
        Float f = Float.valueOf(str);
        return f;
      case DOUBLE:
        Double d = Double.valueOf(str);
        return d;
      case DATE:
        Matcher m = numberPattern.matcher(str);
        Date dt = null;
        if (m.matches()) {
          Long l1 = Long.valueOf(str);
          dt = new Date(l1);
        } else {
          // treat it as date string
          dt = Date.valueOf(str);
        }
        return dt;
      case TIMESTAMP:
        Matcher m2 = numberPattern.matcher(str);
        Timestamp ts = null;
        if (m2.matches()) {
          Long l2 = Long.valueOf(str);
          ts = new Timestamp(l2);
        } else {
          // treat it as date string
          ts = Timestamp.valueOf(str);
        }
        return ts;
      case BOOLEAN:
        Boolean bool = Boolean.valueOf(str);
        return bool;
    }
    return null;
  }

  private List<?> convertJSonListToHCatArray(List<?> arrayVals, HCatFieldSchema hfs) {
    try {
      HCatSchema arraySchema = hfs.getArrayElementSchema();
      HCatFieldSchema elementSchema = arraySchema.getFields().get(0);
      List<Object> result = new ArrayList<Object>();
      for (Object elemVal : arrayVals) {
        LOG.debug("Converting " + elemVal + " of type " + elemVal.getClass().getName() + " to " + elementSchema.getTypeString());
        result.add(toHCat(elemVal, elementSchema));
      }
      return result;
    } catch (Exception e) {
      LOG.error("Unable to convert [" + arrayVals
              + "]  of type " + arrayVals.getClass().getName()
              + " to HCatalog type " + hfs.getTypeString(), e);
    }
    return null;

  }
  private List<?> convertJSonStringToHCatArray(Object obj, HCatFieldSchema hfs) throws IOException {
    LOG.debug("Converting " + obj + " to " + hfs.getTypeString());
    List<?> arrayVals = mapper.readValue(obj.toString(), List.class);
    return convertJSonListToHCatArray(arrayVals, hfs);
  }

  private Map<?, ?> convertJSonMapToHCatMap(Map<?, ?> mapVals, HCatFieldSchema hfs)  {
    LOG.debug("Converting " + mapVals + " to " + hfs.getTypeString());
    try {
      HCatFieldSchema valueSchema = hfs.getMapValueSchema().getFields().get(0);
      HashMap<Object, Object> result = new HashMap<Object, Object>();

      for (Map.Entry<?, ?> e : mapVals.entrySet()) {
        LOG.debug("Converting " + e.getKey() + " of type " + e.getKey().getClass().getName() + " to " + hfs.getTypeString());
        Object key = toHCat(e.getKey(), new HCatFieldSchema("_col", (PrimitiveTypeInfo) hfs.getMapKeyTypeInfo(), ""));
        LOG.debug("Converting " + e.getValue() + " of type " + e.getValue().getClass().getName() + " to " + valueSchema.getTypeString());
        Object val = toHCat(e.getValue(), valueSchema);
        result.put(key, val);
      }
      return result;
    }
    catch (Exception e) {
      LOG.error("Unable to convert [" + mapVals
              + "]  of type " + mapVals.getClass().getName()
              + " to HCatalog type " + hfs.getTypeString(), e);
      LOG.debug(e);
      return null;
    }
  }

  private Map<?, ?> convertJSonStringToHCatMap(Object obj, HCatFieldSchema hfs) throws IOException {
    LOG.debug("Converting " + obj + " to " + hfs.getTypeString());
    Map<?, ?> mapVals = mapper.readValue(obj.toString(), Map.class);
    return convertJSonMapToHCatMap(mapVals, hfs);
  }

  private List<?> convertJSonListToHCatStruct(List<?> structVals, HCatFieldSchema hfs) {
    try {
      HCatSchema structSchema = hfs.getStructSubSchema();
      List<Object> result = new ArrayList<Object>();
      if (structVals.size() != structSchema.getFields().size()) {
        LOG.error("Cannot  convert [" + structVals
                + "]  of type " + structVals.getClass().getName()
                + " to HCatalog type " + hfs.getTypeString() + ". Size mismatch");
      }
      for (int i = 0; i < structVals.size(); ++i) {
        LOG.debug("Converting " + structVals.get(i) + " of type " + structVals.get(i).getClass().getName()
                + " to " + structSchema.getFields().get(i).getTypeString());

        Object val = toHCat(structVals.get(i), structSchema.getFields().get(i));

        result.add(val);
      }
      return result;
    } catch (Exception e) {
      LOG.error("Unable to convert [" + structVals
              + "]  of type " + structVals.getClass().getName()
              + " to HCatalog type " + hfs.getTypeString(), e);
    }
    return null;
  }

  private List<?> convertJSonStringToHCatStruct(Object obj, HCatFieldSchema hfs) throws IOException {
    LOG.debug("Converting " + obj + " to " + hfs.getTypeString());
    List<?> structVals = mapper.readValue(obj.toString(), List.class);
    return convertJSonListToHCatStruct(structVals, hfs);
  }

  private Object convertBooleanToPrimitiveTypes(Object val, PrimitiveTypeInfo typeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();
    Boolean b = (Boolean) val;
    switch(category) {
      case BOOLEAN:
        return b;
      case BYTE:
        return (byte) (b ? 1 : 0);
      case SHORT:
        return (short) (b ? 1 : 0);
      case INT:
        return (int) (b ? 1 : 0);
      case LONG:
        return (long) (b ? 1 : 0);
      case FLOAT:
        return (float) (b ? 1 : 0);
      case DOUBLE:
        return (double) (b ? 1 : 0);
      case STRING:
        return val.toString();
      case VARCHAR:
        VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
        HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
        return hvc;
      case CHAR:
        CharTypeInfo cti = (CharTypeInfo) typeInfo;
        HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
        return hChar;
    }
    return null;
  }

  private Object convertNumberToPrimitiveTypes(Object val, PrimitiveTypeInfo typeInfo) {
    PrimitiveObjectInspector.PrimitiveCategory category = typeInfo.getPrimitiveCategory();

    if (!(val instanceof Number)) {
      return null;
    }

    if (val instanceof BigDecimal
        && category == PrimitiveObjectInspector.PrimitiveCategory.STRING
        || category == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR
        || category == PrimitiveObjectInspector.PrimitiveCategory.CHAR) {
      BigDecimal bd = (BigDecimal) val;
      String bdStr = null;
      if (bigDecimalFormatString) {
        bdStr = bd.toPlainString();
      } else {
        bdStr = bd.toString();
      }
      if (category == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR) {
        VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
        HiveVarchar hvc = new HiveVarchar(bdStr, vti.getLength());
        return hvc;
      } else if (category == PrimitiveObjectInspector.PrimitiveCategory.CHAR) {
        CharTypeInfo cti = (CharTypeInfo) typeInfo;
        HiveChar hChar = new HiveChar(bdStr, cti.getLength());
        return hChar;
      } else {
        return bdStr;
      }
    }
    Number n = (Number) val;
    switch (category) {
      case BYTE:
        return n.byteValue();
      case SHORT:
        return n.shortValue();
      case INT:
        return n.intValue();
      case LONG:
        return n.longValue();
      case FLOAT:
        return n.floatValue();
      case DOUBLE:
        return n.doubleValue();
      case BOOLEAN:
        return n.byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
      case STRING:
        return n.toString();
      case VARCHAR:
        VarcharTypeInfo vti = (VarcharTypeInfo) typeInfo;
        HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
        return hvc;
      case CHAR:
        CharTypeInfo cti = (CharTypeInfo) typeInfo;
        HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
        return hChar;
      case DECIMAL:
        DecimalTypeInfo dti = (DecimalTypeInfo) typeInfo;
        if (n instanceof BigDecimal) {
          return HiveDecimal.create((BigDecimal)n);
        }
        BigDecimal bd = new BigDecimal(n.doubleValue(), new MathContext(dti.precision(), RoundingMode.HALF_UP));
        bd.setScale(dti.scale(), BigDecimal.ROUND_HALF_UP);
        return HiveDecimal.create(bd);
    }
    return null;
  }

  public void cleanup() throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }
}
