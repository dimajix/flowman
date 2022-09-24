/*
 * Copyright 2022 Kaya Kupferschmidt
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

import java.sql.Types

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StringType


class MariaDialect extends MySQLDialect {
    private object Statements extends MySQLStatements(this)
    private object Expressions extends MySQLExpressions(this)
    private object Commands extends MariaCommands(this)

    override def canHandle(url : String): Boolean = url.startsWith("jdbc:mariadb")

    override def getFieldType(sqlType: Int, typeName:String, precision: Int, scale: Int, signed: Boolean): FieldType = {
        if (sqlType == Types.VARCHAR && precision == 65535) {
            StringType
        } else {
            super.getFieldType(sqlType, typeName, precision, scale, signed)
        }
    }

    /**
     * Returns true if a view definition can be changed
     * @return
     */
    override def supportsAlterView : Boolean = true

    override def statement : SqlStatements = Statements
    override def expr : SqlExpressions = Expressions
    override def command : SqlCommands = Commands
}
object MariaDialect extends MariaDialect


class MariaCommands(dialect: BaseDialect) extends MySQLCommands(dialect) {
    override protected def getSchemaName(table: TableIdentifier): String = null

    override protected def getDatabaseName(table: TableIdentifier): String = table.database.orNull
}
