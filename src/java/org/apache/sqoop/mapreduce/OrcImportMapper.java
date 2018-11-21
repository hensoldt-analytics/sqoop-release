/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.util.OrcConversionContext;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class OrcImportMapper
    extends AutoProgressMapper<LongWritable, SqoopRecord, NullWritable, OrcStruct>{

  private LargeObjectLoader lobLoader;
  private OrcStruct orcOutput;
  private final NullWritable nada = NullWritable.get();
  OrcConversionContext occ;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException {
    this.lobLoader = new LargeObjectLoader(context.getConfiguration(), FileOutputFormat.getWorkOutputPath(context));

    String schemaString = context.getConfiguration().get(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute());
    TypeDescription schema = TypeDescription.fromString(schemaString);
    this.orcOutput = (OrcStruct) OrcStruct.createValue(schema);

    occ = new OrcConversionContext(schema);
  }

  @Override
  public void map(LongWritable key, SqoopRecord val, Context context)
      throws IOException, InterruptedException {

    try {
      // Loading of LOBs was delayed until we have a Context.
      val.loadLargeObjects(lobLoader);
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }

    Map<String, Object> fieldMap = val.getFieldMap();
    for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
      String fieldName = e.getKey();
      Object fieldValue = e.getValue();

      WritableComparable wc = occ.convertField(fieldName, fieldValue);
      orcOutput.setFieldValue(fieldName, wc);
    }

    context.write(nada, orcOutput);
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }
}
