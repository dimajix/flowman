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

import java.sql.Statement

import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex


abstract class SqlCommands {
    def createTable(statement:Statement, table:TableDefinition) : Unit
    def dropTable(statement:Statement, table:TableIdentifier) : Unit
    def dropView(statement:Statement, table:TableIdentifier) : Unit

    def getJdbcSchema(statement:Statement, table:TableIdentifier) : Seq[JdbcField]

    def updateComment(statement:Statement, table: TableIdentifier, column:String, comment:Option[String]) : Unit

    def getStorageFormat(statement:Statement, table:TableIdentifier) : Option[String]
    def changeStorageFormat(statement:Statement, table:TableIdentifier, storageFormat:String) : Unit

    def getPrimaryKey(statement:Statement, table:TableIdentifier) : Seq[String]
    def getIndexes(statement:Statement, table:TableIdentifier) : Seq[TableIndex]

    def createIndex(statement:Statement, table:TableIdentifier, index:TableIndex) : Unit
    def dropIndex(statement:Statement, table:TableIdentifier, indexName:String) : Unit

    def dropConstraint(statement:Statement, table:TableIdentifier, constraintName:String) : Unit

    def addPrimaryKey(statement:Statement, table: TableIdentifier, columns:Seq[String]) : Unit
    def dropPrimaryKey(statement:Statement, table: TableIdentifier) : Unit
}
