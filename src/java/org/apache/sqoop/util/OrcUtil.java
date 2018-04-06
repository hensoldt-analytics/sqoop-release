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

package org.apache.sqoop.util;

import org.apache.orc.OrcConf;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.manager.ConnManager;

import java.util.Map;
import java.util.Properties;

public class OrcUtil {

  private static final OrcUtil instance = new OrcUtil();

  private OrcUtil() {}

  public static OrcUtil getInstance() {
    return instance;
  }

  /**
   * Craft ORC schema string based on column types. Columns are stored with their "Hive type" as
   * per {@link ConnManager#toHiveType(String, String, int)}.
   * @param manager
   * @param tableName
   * @param columns
   * @return
   * @throws ImportException
   */
  String createOrcSchemaString(ConnManager manager, String tableName,  String[] columns, Properties overrides)
      throws ImportException {
    Map<String, Integer> columnTypes = manager.getColumnTypes(tableName);
    StringBuilder sb = new StringBuilder("struct<");
    for (String column : columns) {
      sb.append("`");
      sb.append(column);
      sb.append("`:");
      String hiveType = manager.toHiveType(tableName, column, columnTypes.get(column));
      if (overrides.getProperty(column) != null) {
        hiveType = overrides.getProperty(column);
      }
      if (hiveType == null) {
        throw new ImportException("ORC import not supported for column '" + column + "'.");
      }
      sb.append(hiveType);
      sb.append(",");
    }
    if (columns.length > 0) {
      sb.setLength(sb.length() - 1);
    }
    sb.append(">");
    return sb.toString();
  }

  /**
   * Gets column names from connection manager and sets ORC schema in configuration. This is
   * necessary for {@link org.apache.orc.mapreduce.OrcOutputFormat} to work.
   * @param options
   * @param manager
   * @throws ImportException
   */
  public void setOrcSchemaInConf(SqoopOptions options, ConnManager manager) throws ImportException {
    String[] columns = options.getColumns();
    if (columns == null) {
      if (options.getTableName() != null) {
        columns = manager.getColumnNames(options.getTableName());
      } else {
        columns = manager.getColumnNamesForQuery(options.getSqlQuery());
      }
    }

    options.getConf().set(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute(),
        createOrcSchemaString(manager, options.getTableName(), columns, options.getMapColumnHive()));
  }
}
