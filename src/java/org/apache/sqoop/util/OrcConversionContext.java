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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcTimestamp;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Should be used for converting {@link org.apache.sqoop.lib.SqoopRecord}s to
 * {@link org.apache.orc.mapred.OrcStruct}s.
 *
 * Conversion is based on names, so field names in SqoopRecord and Orc schema must
 * match.
 *
 * Instances of this class are NOT thread-safe and should not be reused between different
 * kind of SqoopRecords.
 */
public class OrcConversionContext {
  // Caches converters for fields
  Map<String, OrcConverter> converters = new HashMap<>();

  // Stores ORC output types for fields to be able to get by name
  Map<String, TypeDescription.Category> orcTypes = new HashMap<>();

  public OrcConversionContext(TypeDescription schema) {
    for (int i = 0; i < schema.getFieldNames().size(); i ++) {
      orcTypes.put(schema.getFieldNames().get(i), schema.getChildren().get(i).getCategory());
    }
  }

  @VisibleForTesting
  OrcConversionContext() {}

  /**
   * Implementations of this interface are capable of converting a
   * Java object to a WritableComparable used to set ORC fields.
   */
  public interface OrcConverter {
    WritableComparable convert(Object o);
  }

  /**
   * Returns an {@link OrcConverter} capable of converting the given object to the given
   * ORC type or null if object is null.
   *
   * Should be used via {@link #convertField(String, Object)}
   * @param value
   * @param orcType
   * @return
   */
  @VisibleForTesting
  OrcConverter getConverter(Object value, TypeDescription.Category orcType) {
    if (value == null) {
      return null;
    }
    switch (orcType) {
      case INT:
        return new OrcConverter() {
          private final IntWritable w = new IntWritable();
          @Override public WritableComparable convert(Object o) {
            w.set((Integer) o);
            return w;
          }
        };
      case BYTE: // tinyiny
        return new OrcConverter() {
          private final ByteWritable w = new ByteWritable();
          @Override public WritableComparable convert(Object o) {
            w.set((byte)(int) o);
            return w;
          }
        };
      case LONG: // bigint
        return new OrcConverter() {
          private final LongWritable w = new LongWritable();
          @Override public WritableComparable convert(Object o) {
            w.set((Long) o);
            return w;
          }
        };
      case DOUBLE:
        if (value instanceof Long) {
          return new OrcConverter() {
            private final HiveDecimalWritable w = new HiveDecimalWritable();
            @Override public WritableComparable convert(Object o) {
              w.setFromLong((Long) o);
              return w;
            }
          };
        } else if (value instanceof BigDecimal) {
          return new OrcConverter() {
            private final DoubleWritable w = new DoubleWritable();
            @Override public WritableComparable convert(Object o) {
              w.set(((BigDecimal) o).doubleValue());
              return w;
            }
          };
        } else if (value instanceof Double) {
          return new OrcConverter() {
            private final DoubleWritable w = new DoubleWritable();
            @Override public WritableComparable convert(Object o) {
              w.set((double)o);
              return w;
            }
          };
        } else if (value instanceof Float) {
          return new OrcConverter() {
            private final DoubleWritable w = new DoubleWritable();
            @Override public WritableComparable convert(Object o) {
              w.set((double)(float)o);
              return w;
            }
          };
        } else {
          throw new IllegalArgumentException("Unsupported Java type for DOUBLE column.");
        }
      case STRING:
        return new OrcConverter() {
          private final Text w = new Text();
          @Override public WritableComparable convert(Object o) {
            w.set(o.toString());
            return w;
          }
        };
      case BOOLEAN:
        if (value instanceof Boolean) {
          return new OrcConverter() {
            private final BooleanWritable w = new BooleanWritable();
            @Override public WritableComparable convert(Object o) {
              w.set((boolean) o);
              return w;
            }
          };
        } else {
          throw new IllegalArgumentException("Unsupported Java type for BOOLEAN column.");
        }
      case TIMESTAMP:
        if (value instanceof Date) {
          return new OrcConverter() {
            private final OrcTimestamp w = new OrcTimestamp();
            @Override public WritableComparable convert(Object o) {
              w.setTime(((Date) o).getTime());
              return w;
            }
          };
        } else if (value instanceof Time) {
          return new OrcConverter() {
            private final OrcTimestamp w = new OrcTimestamp();
            @Override public WritableComparable convert(Object o) {
              w.setTime(((Time) o).getTime());
              return w;
            }
          };
        } else if (value instanceof Timestamp) {
          return new OrcConverter() {
            private final OrcTimestamp w = new OrcTimestamp();
            @Override public WritableComparable convert(Object o) {
              w.setTime(((Timestamp) o).getTime());
              return w;
            }
          };
        } else {
          throw new IllegalArgumentException("Unsupported Java type for TIMESTAMP column.");
        }
      case DATE:
        if (value instanceof Date) {
          return new OrcConverter() {
            private final DateWritable w = new DateWritable();
            @Override public WritableComparable convert(Object o) {
              w.set((Date) o);
              return w;
            }
          };
        } else if (value instanceof Timestamp) {
          return new OrcConverter() {
            private final DateWritable w = new DateWritable();
            private final Date dt = new Date(0);
            @Override public WritableComparable convert(Object o) {
              dt.setTime(((Timestamp) o).getTime());
              w.set(dt);
              return w;
            }
          };
        } else {
          throw new IllegalArgumentException("Unsupported Java type for DATE column.");
        }
      default:
        throw new IllegalArgumentException("Unknown category " + orcType);
    }
  }

  /**
   * Convert field to ORC's {@link WritableComparable}.
   * @param fieldName
   * @param fieldValue
   * @return
   */
  public WritableComparable convertField(String fieldName, Object fieldValue) {
    OrcConverter converter = converters.get(fieldName);
    if (fieldValue == null) {
      return null;
    } else {
      if (converter == null) {
        TypeDescription.Category orcType = orcTypes.get(fieldName);
        if (orcType == null) {
          throw new RuntimeException("Unknown field during ORC conversion: " + fieldName);
        }
        converter = getConverter(fieldValue, orcType);
        converters.put(fieldName, converter);
      }
      return converter.convert(fieldValue);
    }
  }
}
