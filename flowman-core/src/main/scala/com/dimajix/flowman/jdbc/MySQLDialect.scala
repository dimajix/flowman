/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import org.apache.spark.sql.catalyst.TableIdentifier


object MySQLDialect extends BaseDialect {
    private object Statements extends MySQLStatements(this)

    override def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql")

    override def quoteIdentifier(colName: String): String = {
        s"`$colName`"
    }

    override def statement : SqlStatements = Statements
}


class MySQLStatements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def firstRow(table: TableIdentifier, condition:String) : String = {
        s"SELECT * FROM ${dialect.quote(table)} WHERE $condition FETCH FIRST ROW ONLY"
    }
}
