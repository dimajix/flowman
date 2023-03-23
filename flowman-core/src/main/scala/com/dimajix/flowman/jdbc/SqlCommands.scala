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

import java.sql.Statement

import com.dimajix.flowman.catalog.PrimaryKey
import com.dimajix.flowman.catalog.TableDefinition
import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.catalog.TableIndex


abstract class SqlCommands {
    def createTable(statement:Statement, table:TableDefinition) : Unit
    def dropTable(statement:Statement, table:TableIdentifier) : Unit
    def createView(statement:Statement, table:TableIdentifier, viewSql:String) : Unit
    def dropView(statement:Statement, table:TableIdentifier) : Unit

    def getJdbcSchema(statement:Statement, table:TableIdentifier) : Seq[JdbcField]

    def addColumn(statement:Statement, table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): Unit
    def deleteColumn(statement:Statement, table: TableIdentifier, columnName: String): Unit
    def updateColumnType(statement:Statement, table: TableIdentifier, columnName: String, newDataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): Unit
    def updateColumnNullability(statement:Statement, table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None): Unit
    def updateColumnComment(statement:Statement, table: TableIdentifier, columnName:String, dataType: String, isNullable: Boolean, charset:Option[String]=None, collation:Option[String]=None, comment:Option[String]=None) : Unit

    def getStorageFormat(statement:Statement, table:TableIdentifier) : Option[String]
    def changeStorageFormat(statement:Statement, table:TableIdentifier, storageFormat:String) : Unit

    def getPrimaryKey(statement:Statement, table:TableIdentifier) : Option[PrimaryKey]
    def getIndexes(statement:Statement, table:TableIdentifier) : Seq[TableIndex]

    def createIndex(statement:Statement, table:TableIdentifier, index:TableIndex) : Unit
    def dropIndex(statement:Statement, table:TableIdentifier, indexName:String) : Unit

    def dropConstraint(statement:Statement, table:TableIdentifier, constraintName:String) : Unit

    def addPrimaryKey(statement:Statement, table: TableIdentifier, pk:PrimaryKey) : Unit
    def dropPrimaryKey(statement:Statement, table: TableIdentifier) : Unit
}
