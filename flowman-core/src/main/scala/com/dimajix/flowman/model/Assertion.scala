/*
 * Copyright 2021 Kaya Kupferschmidt
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


object Assertion {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                kind,
                Map(),
                None
            )
        }
    }

    final case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String],
        description:Option[String]
    ) extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
    }
}


trait Assertion extends Instance {
    override final def category: Category = Category.ASSERTION

    /**
     * Returns a description of the assertion
     * @return
     */
    def description : Option[String]

    /**
     * Returns a list of physical resources required by this assertion. This list will only be non-empty for assertions
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
     * Executes this [[Assertion]] and returns a corresponding DataFrame. This method is explicitly allowed to
     * throw an exception, which will be caught and handled by the [[AssertionRunner]]
     *
     * @param execution
     * @param input
     * @return
     */
    def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : AssertionResult
}


abstract class BaseAssertion extends AbstractInstance with Assertion {
    protected override def instanceProperties : Assertion.Properties

    override def description: Option[String] = instanceProperties.description
}
