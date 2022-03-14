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

package com.dimajix.flowman.catalog

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project


object ExternalCatalog {
    final case class Properties(
        kind:String
    ) extends model.Properties[Properties] {
        override val context : Context = null
        override val namespace : Option[Namespace] = None
        override val project : Option[Project] = None
        override val name : String = ""
        override val metadata : Metadata = Metadata(name="", category=model.Category.EXTERNAL_CATALOG.lower, kind=kind)

        override def withName(name: String): Properties = ???
    }
}


/**
  * This is an interface class to provide external catalogs like Impala or BigSQL with information about
  * changed tables, so they can update their meta data.
  */
trait ExternalCatalog extends Instance {
    override type PropertiesType = ExternalCatalog.Properties

    /**
     * Returns the category of the resource
     *
     * @return
     */
    override def category: Category = Category.EXTERNAL_CATALOG

    def createTable(table: CatalogTable): Unit

    def alterTable(table: CatalogTable): Unit

    def refreshTable(table: CatalogTable): Unit

    def dropTable(table: CatalogTable): Unit

    def truncateTable(table: CatalogTable): Unit

    def addPartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def alterPartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def dropPartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def truncatePartition(table: CatalogTable, partition: CatalogTablePartition): Unit

    def createView(View: CatalogTable): Unit

    def alterView(View: CatalogTable): Unit

    def dropView(View: CatalogTable): Unit
}


abstract class AbstractExternalCatalog extends AbstractInstance with ExternalCatalog {
    override protected def instanceProperties: ExternalCatalog.Properties = ExternalCatalog.Properties(kind)
}
