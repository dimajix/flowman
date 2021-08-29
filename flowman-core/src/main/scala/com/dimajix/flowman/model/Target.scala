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

package com.dimajix.flowman.model

import org.apache.spark.sql.DataFrame

import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.Linker
import com.dimajix.flowman.metric.LongAccumulatorMetric
import com.dimajix.flowman.metric.Selector
import com.dimajix.spark.sql.functions.count_records

/**
  *
  * @param namespace
  * @param project
  * @param target
  * @param partitions
  */
final case class TargetInstance(
    namespace:String,
    project:String,
    target:String,
    partitions:Map[String,String] = Map()
) {
    require(namespace != null)
    require(project != null)
    require(target != null)
    require(partitions != null)

    def asMap: Map[String, String] =
        Map(
            "namespace" -> namespace,
            "project" -> project,
            "name" -> target,
            "target" -> target
        ) ++ partitions
}


object Target {
    object Properties {
        def apply(context: Context, name:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                "",
                Map(),
                Seq(),
                Seq()
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
        before: Seq[TargetIdentifier],
        after: Seq[TargetIdentifier]
    ) extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
        def identifier : TargetIdentifier = TargetIdentifier(name, project.map(_.name))
    }
}


trait Target extends Instance {
    /**
      * Returns the category of this resource
      * @return
      */
    final override def category: String = "target"

    /**
      * Returns an identifier for this target
      * @return
      */
    def identifier : TargetIdentifier

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    def instance : TargetInstance

    /**
      * Returns an explicit user defined list of targets to be executed after this target. I.e. this
      * target needs to be executed before all other targets in this list.
      * @return
      */
    def before : Seq[TargetIdentifier]

    /**
      * Returns an explicit user defined list of targets to be executed before this target I.e. this
      * * target needs to be executed after all other targets in this list.
      *
      * @return
      */
    def after : Seq[TargetIdentifier]

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    def phases : Set[Phase]

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    def provides(phase:Phase) : Set[ResourceIdentifier]

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    def requires(phase:Phase) : Set[ResourceIdentifier]

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     * @param execution
     * @param phase
     * @return
     */
    def dirty(execution: Execution, phase: Phase) : Trilean

    /**
     * Executes a specific phase of this target. This method is explicitly allowed to throw an exception, which will
     * be caught by the [[com.dimajix.flowman.execution.Runner]]
     * @param execution
     * @param phase
     */
    def execute(execution: Execution, phase: Phase) : TargetResult

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    def link(linker:Linker) : Unit
}




abstract class BaseTarget extends AbstractInstance with Target {
    protected override def instanceProperties : Target.Properties

    /**
     * Returns an identifier for this target
     * @return
     */
    override def identifier : TargetIdentifier = instanceProperties.identifier

    /**
     * Returns an instance representing this target with the context
     * @return
     */
    override def instance : TargetInstance = {
        TargetInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name
        )
    }

    /**
     * Returns an explicit user defined list of targets to be executed after this target. I.e. this
     * target needs to be executed before all other targets in this list.
     * @return
     */
    override def before : Seq[TargetIdentifier] = instanceProperties.before

    /**
     * Returns an explicit user defined list of targets to be executed before this target I.e. this
     * * target needs to be executed after all other targets in this list.
     *
     * @return
     */
    override def after : Seq[TargetIdentifier] = instanceProperties.after

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.VALIDATE, Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = Set()

    /**
     * Returns a list of physical resources required by this target
     *
     * @return
     */
    override def requires(phase: Phase): Set[ResourceIdentifier] = Set()


    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = Unknown

    /**
     * Executes a specific phase of this target. This method is explicitly allowed to throw an exception, which will
     * be caught by the [[com.dimajix.flowman.execution.Runner]]
     *
     * @param execution
     * @param phase
     */
    override def execute(execution: Execution, phase: Phase) : TargetResult = {
        phase match {
            case Phase.VALIDATE => validate(execution)
            case Phase.CREATE => create(execution)
            case Phase.BUILD => build(execution)
            case Phase.VERIFY => verify(execution)
            case Phase.TRUNCATE => truncate(execution)
            case Phase.DESTROY => destroy(execution)
        }

        TargetResult(this, phase, Status.SUCCESS)
    }

    /**
     * Creates all known links for building a descriptive graph of the whole data flow
     * Params: linker - The linker object to use for creating new edges
     */
    override def link(linker:Linker) : Unit = {}

    /**
     * Performs validation before execution. This might be a good point in time to validate any
     * assumption on data sources
     */
    protected def validate(executor:Execution) : Unit = {}

    /**
     * Creates the resource associated with this target. This may be a Hive table or a JDBC table. This method
     * will not provide the data itself, it will only create the container
     * @param executor
     */
    protected def create(executor:Execution) : Unit = {}

    /**
     * Abstract method which will perform the output operation. All required tables need to be
     * registered as temporary tables in the Spark session before calling the execute method.
     *
     * @param executor
     */
    protected def build(executor:Execution) : Unit = {}

    /**
     * Performs a verification of the build step or possibly other checks.
     *
     * @param executor
     */
    protected def verify(executor: Execution) : Unit = {}

    /**
     * Deletes data of a specific target
     *
     * @param executor
     */
    protected def truncate(executor:Execution) : Unit = {}

    /**
     * Completely destroys the resource associated with this target. This will delete both the phyiscal data and
     * the table definition
     * @param executor
     */
    protected def destroy(executor:Execution) : Unit = {}

    protected def countRecords(executor:Execution, df:DataFrame) : DataFrame = {
        val counter = executor.metrics.findMetric(Selector(Some("target_records"), metadata.asMap))
            .headOption
            .map(_.asInstanceOf[LongAccumulatorMetric].counter)
            .getOrElse {
                val counter = executor.spark.sparkContext.longAccumulator
                val metric = LongAccumulatorMetric("target_records", metadata.asMap, counter)
                executor.metrics.addMetric(metric)
                counter
            }

        count_records(df, counter)
    }
}
