/*
 * Copyright 2019-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.model

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.NoSuchMappingOutputException
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.DataFrameUtils


object Mapping {
    object Properties {
        def apply(context: Context, name: String = ""): Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map(),
                false,
                false,
                StorageLevel.NONE
            )
        }
    }
    final case class Properties(
        context: Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String],
        broadcast:Boolean,
        checkpoint:Boolean,
        cache:StorageLevel
    ) extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
    }
}


trait Mapping extends Instance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "mapping"

    /**
      * Returns an identifier for this mapping
      * @return
      */
    def identifier : MappingIdentifier

    /**
      * This method should return true, if the resulting dataframe should be broadcast for map-side joins
      * @return
      */
    def broadcast : Boolean

    /**
      * This method should return true, if the resulting dataframe should be checkpointed
      * @return
      */
    def checkpoint : Boolean

    /**
      * Returns the desired storage level. Default should be StorageLevel.NONE
      * @return
      */
    def cache : StorageLevel

    /**
      * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
      * which actually read from physical data.
      * @return
      */
    def requires : Set[ResourceIdentifier]

    /**
      * Returns the dependencies (i.e. names of tables in the Dataflow model)
      * @return
      */
    def inputs : Seq[MappingOutputIdentifier]

    /**
      * Lists all outputs of this mapping. Every mapping should have one "main" output
      * @return
      */
    def outputs : Seq[String]

    /**
     * Creates an output identifier for the primary output
     * @return
     */
    def output : MappingOutputIdentifier

    /**
     * Creates an output identifier for the specified output name
     * @param name
     * @return
     */
    def output(name:String = "main") : MappingOutputIdentifier

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    def execute(executor:Executor, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame]

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType]

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType], output:String) : StructType
}


/**
 * Common base implementation for the MappingType interface
 */
abstract class BaseMapping extends AbstractInstance with Mapping {
    protected override def instanceProperties : Mapping.Properties

    /**
     * Returns an identifier for this mapping
     * @return
     */
    override def identifier : MappingIdentifier = MappingIdentifier(name, project.map(_.name))

    /**
     * This method should return true, if the resulting dataframe should be broadcast for map-side joins
     * @return
     */
    override def broadcast : Boolean = instanceProperties.broadcast

    /**
     * This method should return true, if the resulting dataframe should be checkpointed
     * @return
     */
    override def checkpoint : Boolean = instanceProperties.checkpoint

    /**
     * Returns the desired storage level. Default should be StorageLevel.NONE
     * @return
     */
    override def cache : StorageLevel = instanceProperties.cache

    /**
     * Returns a list of physical resources required by this mapping. This list will only be non-empty for mappings
     * which actually read from physical data.
     * @return
     */
    override def requires : Set[ResourceIdentifier] = Set()

    /**
     * Lists all outputs of this mapping. Every mapping should have one "main" output
     * @return
     */
    override def outputs : Seq[String] = Seq("main")

    /**
     * Creates an output identifier for the primary output
     * @return
     */
    override def output : MappingOutputIdentifier = {
        MappingOutputIdentifier(identifier, "main")
    }

    /**
     * Creates an output identifier for the specified output name
     * @param name
     * @return
     */
    override def output(name:String = "main") : MappingOutputIdentifier = {
        if (!outputs.contains(name))
            throw new NoSuchMappingOutputException(identifier, name)
        MappingOutputIdentifier(identifier, name)
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. The map might not contain
     * schema information for all outputs, if the schema cannot be inferred.
     * @param input
     * @return
     */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(executor != null)
        require(input != null)

        // Create dummy data frames
        val replacements = input.map { case (name,schema) =>
            name -> DataFrameUtils.singleRow(executor.spark, schema.sparkType)
        }

        // Execute mapping
        val results = execute(executor, replacements)

        // Extract schemas
        results.map { case (name,df) => name -> StructType.of(df.schema)}
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. If the schema cannot
     * be inferred, None will be returned
     * @param input
     * @return
     */
    override def describe(executor:Executor, input:Map[MappingOutputIdentifier,StructType], output:String) : StructType = {
        require(executor != null)
        require(input != null)
        require(output != null && output.nonEmpty)

        describe(executor, input)(output)
    }
}
