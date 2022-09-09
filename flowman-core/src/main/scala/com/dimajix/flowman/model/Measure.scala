/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.model


object Measure {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                Metadata(context, name, Category.MEASURE, kind),
                None
            )
        }
    }

    final case class Properties(
        context:Context,
        metadata:Metadata,
        description:Option[String]
    ) extends model.Properties[Properties] {
        require(metadata.category == Category.MEASURE.lower)
        require(metadata.namespace == context.namespace.map(_.name))
        require(metadata.project == context.project.map(_.name))
        require(metadata.version == context.project.flatMap(_.version))

        override val namespace : Option[Namespace] = context.namespace
        override val project : Option[Project] = context.project
        override val kind : String = metadata.kind
        override val name : String = metadata.name

        override def withName(name: String): Properties = copy(metadata=metadata.copy(name = name))

        def merge(other: Properties): Properties = {
            Properties(context, metadata.merge(other.metadata), description.orElse(other.description))
        }
        def identifier : MeasureIdentifier = MeasureIdentifier(name, project.map(_.name))
    }
}


trait Measure extends Instance {
    override type PropertiesType = Measure.Properties

    /**
     * Returns the category of this resource
     * @return
     */
    final override def category: Category = Category.MEASURE

    /**
     * Returns an identifier for this measure
     * @return
     */
    def identifier : MeasureIdentifier

    /**
     * Returns a description of the measure
     * @return
     */
    def description : Option[String]

    /**
     * Returns a list of physical resources required by this measure. This list will only be non-empty for assertions
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
     * Executes this [[Assertion]] and returns a corresponding DataFrame.
     *
     * @param execution
     * @param input
     * @return
     */
    def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : MeasureResult
}
abstract class BaseMeasure extends AbstractInstance with Measure {
    protected override def instanceProperties : Measure.Properties

    /**
     * Returns an identifier for this assertion
     * @return
     */
    override def identifier : MeasureIdentifier = instanceProperties.identifier

    override def description: Option[String] = instanceProperties.description
}
