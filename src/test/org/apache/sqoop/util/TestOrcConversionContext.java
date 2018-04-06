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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcTimestamp;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.CubridManager;
import org.apache.sqoop.manager.Db2Manager;
import org.apache.sqoop.manager.GenericJdbcManager;
import org.apache.sqoop.manager.HsqldbManager;
import org.apache.sqoop.manager.MySQLManager;
import org.apache.sqoop.manager.NetezzaManager;
import org.apache.sqoop.manager.OracleManager;
import org.apache.sqoop.manager.PostgresqlManager;
import org.apache.sqoop.manager.SQLServerManager;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestOrcConversionContext {

  private static final double DOUBLE_DELTA = 1e-10;

  private OrcConversionContext uut;

  @Before
  public void setUp() {
    uut = new OrcConversionContext();
  }

  @Test
  public void testGetConverterSimpleTypes() throws Exception {
    assertEquals(5, ((IntWritable)uut.getConverter(5, TypeDescription.Category.INT).convert(5)).get());
    assertEquals(5, ((ByteWritable)uut.getConverter(5, TypeDescription.Category.BYTE).convert(5)).get());
    assertEquals("somestring", ((Text)uut.getConverter("somestring", TypeDescription.Category.STRING).convert("somestring")).toString());
    assertEquals(10l, ((LongWritable)uut.getConverter(10l, TypeDescription.Category.LONG).convert(10l)).get());
    assertEquals(2.72, ((DoubleWritable)uut.getConverter(2.72, TypeDescription.Category.DOUBLE).convert(2.72)).get(), DOUBLE_DELTA);
    assertEquals(2.72f, ((DoubleWritable)uut.getConverter(2.72f, TypeDescription.Category.DOUBLE).convert(2.72f)).get(), DOUBLE_DELTA);
    assertEquals(true, ((BooleanWritable)uut.getConverter(true, TypeDescription.Category.BOOLEAN).convert(true)).get());
  }

  @Test
  public void testGetConverterDatetimeTypes() throws Exception {
    TimeZone originalTZ = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

      Date dt = new Date(1523404800000l);
      Timestamp ts = new Timestamp(1523444288000l);

      // As string
      assertEquals("2018-04-11", ((Text)uut.getConverter(dt, TypeDescription.Category.STRING).convert(dt)).toString());
      assertEquals("2018-04-11 10:58:08.0", ((Text)uut.getConverter(ts, TypeDescription.Category.STRING).convert(ts)).toString());

      // As datetime types
      assertEquals(dt, ((DateWritable)uut.getConverter(dt, TypeDescription.Category.DATE).convert(dt)).get());
      assertEquals(ts.getTime(), ((OrcTimestamp)uut.getConverter(ts, TypeDescription.Category.TIMESTAMP).convert(ts)).getTime());
      assertEquals(0, ((OrcTimestamp)uut.getConverter(ts, TypeDescription.Category.TIMESTAMP).convert(ts)).getNanos());
    } finally {
      TimeZone.setDefault(originalTZ);
    }
  }

  @Test
  public void testConvertFieldNullWithoutConverter() throws Exception {
    TypeDescription schema = TypeDescription.createStruct();
    for (TypeDescription.Category c: TypeDescription.Category.values()) {
      schema.addField(c.getName(), new TypeDescription(c));
    }
    uut = new OrcConversionContext(schema);
    for (TypeDescription.Category c: TypeDescription.Category.values()) {
      assertEquals(null, uut.convertField(c.getName(), null));
    }
    assertEquals(0, uut.converters.size());
  }

  @Test
  public void testConvertFieldUpdatesConverters() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<name:INT>");
    uut = new OrcConversionContext(schema);
    assertEquals(0, uut.converters.size());
    assertEquals(5, ((IntWritable)uut.convertField("name", 5)).get());
    assertEquals(1, uut.converters.size());
    assertEquals(null, uut.convertField("name", null));
    assertEquals(1, uut.converters.size());
  }

  @Test
  public void testConverterReuse() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<name:INT>");
    uut = new OrcConversionContext(schema);
    assertEquals(5, ((IntWritable)uut.convertField("name", 5)).get());
    assertEquals(6, ((IntWritable)uut.convertField("name", 6)).get());
    assertEquals(7, ((IntWritable)uut.convertField("name", 7)).get());
    assertEquals(1, uut.converters.size());
  }

  @Test(expected = RuntimeException.class)
  public void testConvertFieldUnknownField() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<name:INT>");
    uut = new OrcConversionContext(schema);

    uut.convertField("unknownfield", 5);
  }

  private Object getTestObjectForType(String type) {
    switch (type) {
      case "String":
        return "teststring";
      case "Integer":
        return 5;
      case "java.math.BigDecimal":
        return new BigDecimal("2.72");
      case "Boolean":
        return true;
      case "Long":
        return 10l;
      case "Float":
        return 3.14f;
      case "Double":
        return 3.14159265d;
      case "java.sql.Date":
        return new Date(1523444288000l);
      case "java.sql.Time":
        return new Time(1523444288000l);
      case "java.sql.Timestamp":
        return new Timestamp(1523444288000l);
      case "org.apache.sqoop.lib.ClobRef":
        return new ClobRef("testclob");
      default:
        throw new RuntimeException("No test data for type: " + type);
    }
  }

  /**
   * Test that {@link OrcConversionContext#getConverter(Object, TypeDescription.Category)} is able to handle
   * all combinations of Java and Hive types created by connection managers.
   * @throws Exception
   */
  @Test
  public void testConvertersWithDefaultTypesOfConnManagers() throws Exception {
    Configuration dummyConf = new Configuration();
    SqoopOptions options = mock(SqoopOptions.class);
    when(options.getConf()).thenReturn(dummyConf);

    List<ConnManager> managers = new ArrayList<>();
    managers.add(new GenericJdbcManager("nosuchclass", options));
    managers.add(new Db2Manager(options));
    managers.add(new OracleManager(options));
    managers.add(new HsqldbManager(options));
    managers.add(new PostgresqlManager(options));
    managers.add(new CubridManager(options));
    managers.add(new SQLServerManager(options));
    managers.add(new MySQLManager(options));
    managers.add(new NetezzaManager(options));

    // Try all SQL types
    List<Integer> sqlTypes = new ArrayList<>();
    for (Field field : Types.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())
              && field.getType() == int.class) {
        sqlTypes.add((int)field.get(null));
      }
    }

    for (ConnManager manager : managers) {
      for (int type : sqlTypes) {
        if (manager.toHiveType(type) == null) {
          continue; // null Hive type means type is not supported => skip
        }

        String javaType = manager.toJavaType(type);
        Object fieldValue = getTestObjectForType(javaType);
        String hiveType = manager.toHiveType(type);
        TypeDescription.Category orcType = TypeDescription.fromString(hiveType).getCategory();

        // Only thing we verify is that the Javatype+Hivetype combination
        // didn't result in an exception.
        uut.getConverter(fieldValue, orcType).convert(fieldValue);
      }
    }
  }

}
