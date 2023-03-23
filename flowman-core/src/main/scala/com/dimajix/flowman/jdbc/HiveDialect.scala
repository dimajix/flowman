/*
 * Copyright (C) 2018 The Flowman Authors
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


class HiveDialect extends BaseDialect {
    override def canHandle(url : String): Boolean = url.startsWith("jdbc:hive")

    def quote(table:org.apache.spark.sql.catalyst.TableIdentifier): String = {
        table.database.map(db => quoteIdentifier(db) + "." + quoteIdentifier(table.table))
            .getOrElse(quoteIdentifier(table.table))
    }

    /**
      * Quotes the identifier. This is used to put quotes around the identifier in case the column
      * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
      */
    override def quoteIdentifier(colName: String): String = {
        s"""`$colName`"""
    }

    /**
     * Returns true if the SQL database supports retrieval of the exact view definition
     *
     * @return
     */
    override def supportsExactViewRetrieval: Boolean = true
}
object HiveDialect extends HiveDialect
