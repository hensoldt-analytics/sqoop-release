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

package org.apache.sqoop.util;

import java.sql.SQLException;

import org.apache.commons.logging.Log;

/**
 * A helper class for logging.
 */
public final class LoggingUtils {

  private LoggingUtils() { }

  /**
   * Log every exception in the chain if
   * the exception is a chain of exceptions.
   */
  public static void logAll(Log log, SQLException e) {
    log.error("Top level exception: ", e);
    e = e.getNextException();
    int indx = 1;
    while (e != null) {
      log.error("Chained exception " + indx + ": ", e);
      e = e.getNextException();
      indx++;
    }
  }

}

