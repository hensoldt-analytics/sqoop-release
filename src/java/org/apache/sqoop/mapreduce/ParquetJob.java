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

package org.apache.sqoop.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.sqoop.avro.AvroSchemaMismatchException;
import org.apache.sqoop.hive.HiveConfig;
import org.kitesdk.data.CompressionType;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;
import org.kitesdk.data.spi.SchemaValidationUtil;

import java.io.IOException;

/**
 * Helper class for setting up a Parquet MapReduce job.
 */
public final class ParquetJob {

  public static final Log LOG = LogFactory.getLog(ParquetJob.class.getName());

  public static final String HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
  // Purposefully choosing the same token alias as the one Oozie chooses.
  // Make sure we don't generate a new delegation token if oozie
  // has already generated one.
  public static final String HIVE_METASTORE_TOKEN_ALIAS = "HCat Token";

  public static final String INCOMPATIBLE_AVRO_SCHEMA_MSG = "Target dataset was created with an incompatible Avro schema. ";

  public static final String HIVE_INCOMPATIBLE_AVRO_SCHEMA_MSG = "You tried to import to an already existing Hive table in " +
      "Parquet format. Sqoop maps date/timestamp SQL types to int/bigint Hive types during Hive Parquet import" +
      " but it is possible that date/timestamp types were mapped to strings during table" +
      " creation. Consider using Sqoop option --map-column-java resolve the mismatch" +
      " (e.g. --map-column-java date_field1=String,timestamp_field1=String).";

  private static final String HIVE_URI_PREFIX = "dataset:hive";

  private ParquetJob() {
  }

  private static final String CONF_AVRO_SCHEMA = "parquetjob.avro.schema";
  static final String CONF_OUTPUT_CODEC = "parquetjob.output.codec";
  enum WriteMode {
    DEFAULT, APPEND, OVERWRITE
  };

  public static Schema getAvroSchema(Configuration conf) {
    return new Schema.Parser().parse(conf.get(CONF_AVRO_SCHEMA));
  }

  public static CompressionType getCompressionType(Configuration conf) {
    CompressionType defaults = Formats.PARQUET.getDefaultCompressionType();
    String codec = conf.get(CONF_OUTPUT_CODEC, defaults.getName());
    try {
      return CompressionType.forName(codec);
    } catch (IllegalArgumentException ex) {
      LOG.warn(String.format(
          "Unsupported compression type '%s'. Fallback to '%s'.",
          codec, defaults));
    }
    return defaults;
  }

  /**
   * Configure the import job. The import process will use a Kite dataset to
   * write data records into Parquet format internally. The input key class is
   * {@link org.apache.sqoop.lib.SqoopRecord}. The output key is
   * {@link org.apache.avro.generic.GenericRecord}.
   */
  public static void configureImportJob(JobConf conf, Schema schema,
      String uri, WriteMode writeMode, String externalHiveDir) throws IOException {
    Dataset dataset;

    // Add hive delegation token only if we don't already have one.
    if (isHiveImport(uri)) {
      Configuration hiveConf = HiveConfig.getHiveConf(conf);
      if (isSecureMetastore(hiveConf)) {
        // Copy hive configs to job config
        HiveConfig.addHiveConfigs(hiveConf, conf);

        if (conf.getCredentials().getToken(new Text(HIVE_METASTORE_TOKEN_ALIAS)) == null) {
          addHiveDelegationToken(conf);
        }
      }
    }

    if (Datasets.exists(uri)) {
      if (WriteMode.DEFAULT.equals(writeMode)) {
        throw new IOException("Destination exists! " + uri);
      }

      dataset = Datasets.load(uri);
      Schema writtenWith = dataset.getDescriptor().getSchema();
      if (!SchemaValidationUtil.canRead(writtenWith, schema)) {
        String exceptionMessage = buildAvroSchemaMismatchMessage(isHiveImport(uri));
        throw new AvroSchemaMismatchException(exceptionMessage, writtenWith, schema);
      }
    } else {
      dataset = createDataset(schema, getCompressionType(conf), uri, externalHiveDir);
    }
    conf.set(CONF_AVRO_SCHEMA, schema.toString());

    DatasetKeyOutputFormat.ConfigBuilder builder =
        DatasetKeyOutputFormat.configure(conf);
    if (WriteMode.OVERWRITE.equals(writeMode)) {
      builder.overwrite(dataset);
    } else if (WriteMode.APPEND.equals(writeMode)) {
      builder.appendTo(dataset);
    } else {
      builder.writeTo(dataset);
    }
  }

  private static boolean isHiveImport(String importUri) {
    return importUri.startsWith(HIVE_URI_PREFIX);
  }

  public static Dataset createDataset(Schema schema,
      CompressionType compressionType, String uri, String externalHiveDir) {
    DatasetDescriptor.Builder builder = new DatasetDescriptor.Builder()
        .schema(schema)
        .format(Formats.PARQUET)
        .compressionType(compressionType);
    if ( !StringUtils.isBlank(externalHiveDir)) {
      builder.location(externalHiveDir);
    }
    DatasetDescriptor descriptor = builder.build();
    return Datasets.create(uri, descriptor, GenericRecord.class);
  }

  private static boolean isSecureMetastore(Configuration conf) {
    return conf != null && conf.getBoolean(HIVE_METASTORE_SASL_ENABLED, false);
  }

  /**
   * Add hive delegation token to credentials store.
   * @param conf
   */
  private static void addHiveDelegationToken(JobConf conf) {
    HiveConf hiveConf = new HiveConf(conf, Configuration.class);
    try {
      IMetaStoreClient client = HCatUtil.getHiveMetastoreClient(hiveConf);
      String owner = SecurityUtils.getUser();
      String kerberosPrincipal = UserGroupInformation.getLoginUser().getShortUserName();
      String tokenStringForm = client.getDelegationToken(owner, kerberosPrincipal);

      Token<DelegationTokenIdentifier> metastoreToken = new Token<>();
      metastoreToken.decodeFromUrlString(tokenStringForm);
      conf.getCredentials().addToken(new Text(HIVE_METASTORE_TOKEN_ALIAS), metastoreToken);

      LOG.debug("Successfully fetched hive metastore delegation token. " + metastoreToken);
    } catch (Exception ex) {
      LOG.error("Couldn't fetch delegation token.", ex);
      throw new RuntimeException("Couldn't fetch delegation token.", ex);
    }
  }

  private static String buildAvroSchemaMismatchMessage(boolean hiveImport) {
    String exceptionMessage = INCOMPATIBLE_AVRO_SCHEMA_MSG;

    if (hiveImport) {
      exceptionMessage += HIVE_INCOMPATIBLE_AVRO_SCHEMA_MSG;
    }

    return exceptionMessage;
  }

}
