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

import org.apache.sqoop.manager.ConnManager;
import org.junit.Test;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalAnswers.returnsSecondArg;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestOrcUtil {

  private OrcUtil uut = OrcUtil.getInstance();

  @Test
  public void testCreateOrcSchemaString() throws Exception {
    Map<String, Integer> columnTypes = new HashMap<>();
    columnTypes.put("c0", Types.SMALLINT);
    columnTypes.put("c1", Types.VARCHAR);
    columnTypes.put("c2", Types.DECIMAL);

    ConnManager manager = mock(ConnManager.class);
    when(manager.getColumnTypes(anyString())).thenReturn(columnTypes);
    when(manager.toHiveType(anyString(), anyString(), anyInt())).then(returnsSecondArg());
    String schemaString =
        uut.createOrcSchemaString(manager, "table", columnTypes.keySet().toArray(new String[columnTypes.size()]), new Properties());

    assertEquals("struct<`c0`:c0,`c1`:c1,`c2`:c2>", schemaString);
  }

  @Test
  public void testCreateOrcSchemaStringNoColumns() throws Exception {
    ConnManager manager = mock(ConnManager.class);
    String schemaString =
        uut.createOrcSchemaString(manager, "table", new String[0], new Properties());

    assertEquals("struct<>", schemaString);
  }
}
