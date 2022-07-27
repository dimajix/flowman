/*
 * Copyright 2018-2022 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.jdbc


object SqlDialects {
    /**
      * Register a dialect for use on all new matching jdbc `org.apache.spark.sql.DataFrame`.
      * Reading an existing dialect will cause a move-to-front.
      *
      * @param dialect The new dialect.
      */
    def registerDialect(dialect: SqlDialect) : Unit = {
        dialects = dialect :: dialects.filterNot(_ == dialect)
    }

    /**
      * Unregister a dialect. Does nothing if the dialect is not registered.
      *
      * @param dialect The jdbc dialect.
      */
    def unregisterDialect(dialect : SqlDialect) : Unit = {
        dialects = dialects.filterNot(_ == dialect)
    }

    private[this] var dialects = List[SqlDialect]()

    registerDialect(HiveDialect)
    registerDialect(DerbyDialect)
    registerDialect(H2Dialect)
    registerDialect(MySQLDialect)
    registerDialect(SqlServerDialect)
    registerDialect(PostgresDialect)
    registerDialect(OracleDialect)

    /**
      * Fetch the JdbcDialect class corresponding to a given database url.
      */
    def get(url: String): SqlDialect = {
        val matchingDialects = dialects.filter(_.canHandle(url))
        matchingDialects.length match {
            case 0 => NoopDialect
            case 1 => matchingDialects.head
            case _ => matchingDialects.head
        }
    }
}
