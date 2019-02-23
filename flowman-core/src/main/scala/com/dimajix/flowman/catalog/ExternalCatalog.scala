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

package com.dimajix.flowman.catalog

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition


/**
  * This is an interface class to provide external catalogs like Impala or BigSQL with information about
  * changed tables, so they can update their meta data.
  */
abstract class ExternalCatalog {
    def createTable(table: CatalogTable): Unit

    def alterTable(table: CatalogTable): Unit

    def dropTable(table: CatalogTable): Unit

    def truncateTable(table: CatalogTable): Unit

    def addPartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def alterPartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def dropPartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def truncatePartition(table: CatalogTable, partition: CatalogTablePartition): Unit
}
