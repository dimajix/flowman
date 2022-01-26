/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.NoSuchMappingOutputException
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.sql.DataFrameBuilder


object Mapping {
    object Properties {
        def apply(context: Context, name: String = "", kind:String = ""): Properties = {
            Properties(
                context,
                Metadata(context, name, Category.MAPPING, kind),
                false,
                false,
                StorageLevel.NONE
            )
        }
    }
    final case class Properties(
        context: Context,
        metadata:Metadata,
        broadcast:Boolean,
        checkpoint:Boolean,
        cache:StorageLevel
    ) extends Instance.Properties[Properties] {
        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))
        def identifier : MappingIdentifier = MappingIdentifier(name, project.map(_.name))
    }
}


trait Mapping extends Instance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: Category = Category.MAPPING

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
     * Lists all outputs of this mapping. Every mapping should have one "main" output, which is the default output
     * implicitly used when no output is specified. But eventually, the "main" output is not mandatory, but
     * recommended.
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
      * Executes this Mapping and returns a corresponding map of DataFrames per output. The map should contain
      * one entry for each declared output in [[outputs]]. If it contains an additional entry called `cache`, then
      * this [[DataFrame]] will be cached instead of all outputs. The `cache` DataFrame may even well be some
      * internal [[DataFrame]] which is not listed in [[outputs]].
      *
      * @param execution
      * @param input
      * @return
      */
    def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : Map[String,DataFrame]

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema. The method should
      * return one entry for each entry declared in [[outputs]].
      * @param input
      * @return
      */
    def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType]

    /**
      * Returns the schema as produced by this mapping, relative to the given input schema
      * @param input
      * @return
      */
    def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType], output:String) : StructType

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    def link(linker:Linker) : Unit
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
    override def identifier : MappingIdentifier = instanceProperties.identifier

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
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType]) : Map[String,StructType] = {
        require(execution != null)
        require(input != null)

        // Create dummy data frames
        val replacements = input.map { case (name,schema) =>
            name -> DataFrameBuilder.singleRow(execution.spark, schema.sparkType)
        }

        // Execute mapping
        val results = execute(execution, replacements)

        // Extract schemas
        results.map { case (name,df) => name -> StructType.of(df.schema)}
    }

    /**
     * Returns the schema as produced by this mapping, relative to the given input schema. If the schema cannot
     * be inferred, None will be returned
     * @param input
     * @return
     */
    override def describe(execution:Execution, input:Map[MappingOutputIdentifier,StructType], output:String) : StructType = {
        require(execution != null)
        require(input != null)
        require(output != null && output.nonEmpty)

        describe(execution, input)(output)
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker:Linker) : Unit = {
        inputs.foreach( in =>
            linker.input(in.mapping, in.output)
        )
    }
}
