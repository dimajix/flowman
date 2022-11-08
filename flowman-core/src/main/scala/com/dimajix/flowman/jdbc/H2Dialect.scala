/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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
import java.util.Locale

import com.dimajix.flowman.catalog.TableIdentifier


class H2Dialect extends BaseDialect {
    private object Statements extends H2Statements(this)

    override def canHandle(url: String): Boolean = url.startsWith("jdbc:h2")

    /**
     * Quotes the identifier. This is used to put quotes around the identifier in case the column
     * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
     */
    override def quoteIdentifier(colName: String): String = {
        s"""`$colName`"""
    }

    /**
     * Returns true if a view definition can be changed
     * @return
     */
    override def supportsAlterView : Boolean = true

    override def statement : SqlStatements = Statements
}
object H2Dialect extends H2Dialect


class H2Statements(dialect: BaseDialect) extends BaseStatements(dialect)  {
    override def getViewDefinition(table: TableIdentifier): String = {
        s"""
          |SELECT
          |    VIEW_DEFINITION
          |FROM INFORMATION_SCHEMA.VIEWS
          |WHERE TABLE_SCHEMA = ${table.space.headOption.map(s => dialect.literal(s.toUpperCase(Locale.ROOT))).getOrElse("current_schema")}
          |    AND TABLE_NAME = ${dialect.literal(table.table.toUpperCase(Locale.ROOT))}
          |""".stripMargin
    }
}
