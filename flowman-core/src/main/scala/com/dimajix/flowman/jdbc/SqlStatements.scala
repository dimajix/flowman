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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.MergeClause


abstract class SqlStatements {
    /**
     * The SQL query that should be used to discover the schema of a table. It only needs to
     * ensure that the result set has the same schema as the table, such as by calling
     * "SELECT * ...". Dialects can override this method to return a query that works best in a
     * particular database.
     * @param table The name of the table.
     * @return The SQL query to use for discovering the schema.
     */
    def schema(table: TableIdentifier): String

    /**
     * The SQL query for creating a new table
     * @param table
     * @return
     */
    def createTable(table: TableDefinition): String

    /**
     * Get the SQL query that should be used to find if the given table exists. Dialects can
     * override this method to return a query that works best in a particular database.
     * @param table  The name of the table.
     * @return The SQL query to use for checking the table.
     */
    def tableExists(table: TableIdentifier) : String

    def firstRow(table: TableIdentifier, condition:String) : String

    def addColumn(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean): String
    def renameColumn(table: TableIdentifier, columnName: String, newName: String) : String
    def deleteColumn(table: TableIdentifier, columnName: String): String
    def updateColumnType(table: TableIdentifier, columnName: String, newDataType: String): String
    def updateColumnNullability(table: TableIdentifier, columnName: String, dataType: String, isNullable: Boolean): String

    def merge(table: TableIdentifier, targetAlias:String, targetSchema:Option[StructType], sourceAlias:String, sourceSchema:StructType, condition:Column, clauses:Seq[MergeClause]) : String
}
