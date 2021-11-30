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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Job.Parameter
import com.dimajix.flowman.types.FieldType

final case class TestInstance(
    namespace:String,
    project:String,
    test:String
) {
    require(namespace != null)
    require(project != null)
    require(test != null)

    def asMap: Map[String, String] =
        Map(
            "namespace" -> namespace,
            "project" -> project,
            "name" -> test,
            "test" -> test
        )
}


object Test {
    object Properties {
        def apply(context: Context, name: String = ""): Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                Map(),
                None
            )
        }
    }
    final case class Properties(
        context: Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name: String,
        labels: Map[String, String],
        description:Option[String]
    ) extends Instance.Properties[Properties] {
        override val kind : String = "test"
        override def withName(name: String): Properties = copy(name=name)
    }

    class Builder(context:Context) {
        require(context != null)
        private var name:String = ""
        private var labels:Map[String,String] = Map()
        private var description:Option[String] = None
        private var targets:Seq[TargetIdentifier] = Seq()
        private var environment:Map[String,String] = Map()

        def build() : Test = Test(
            Test.Properties(context, context.namespace, context.project, name, labels, description),
            environment = environment,
            targets = targets
        )
        def setProperties(props:Test.Properties) : Builder = {
            require(props != null)
            require(props.context eq context)
            name = props.name
            labels = props.labels
            description = props.description
            this
        }
        def setName(name:String) : Builder = {
            require(name != null)
            this.name = name
            this
        }
        def setDescription(desc:String) : Builder = {
            require(desc != null)
            this.description = Some(desc)
            this
        }
        def setEnvironment(env:Map[String,String]) : Builder = {
            require(env != null)
            this.environment = env
            this
        }
        def addEnvironment(key:String, value:String) : Builder = {
            require(key != null)
            require(value != null)
            this.environment = this.environment + (key -> value)
            this
        }
        def setTargets(targets:Seq[TargetIdentifier]) : Builder = {
            require(targets != null)
            this.targets = targets
            this
        }
        def addTarget(target:TargetIdentifier) : Builder = {
            require(target != null)
            this.targets = this.targets :+ target
            this
        }
    }

    def builder(context: Context) : Builder = new Builder(context)


    /**
     * Creates a new [[Test]] from an existing test and a list of parent tests
     * @param test
     * @param parents
     * @return
     */
    def merge(test:Test, parents:Seq[Test]) : Test = {
        val parentEnvironment = parents
            .map(test => test.environment)
            .reduceOption((envs, elems) => envs ++ elems)
            .getOrElse(Map())
        val parentTargets = parents
            .map(test => test.targets)
            .reduceOption((targets, elems) => targets ++ elems)
            .getOrElse(Seq())

        val allEnvironment = parentEnvironment ++ test.environment

        val allTargets = (parentTargets ++ test.targets).distinct
        val allRelationMocks = parents.foldLeft(Map[String,Prototype[Relation]]())((f, t) => f ++ t.overrideRelations) ++ test.overrideRelations
        val allMappingMocks = parents.foldLeft(Map[String,Prototype[Mapping]]())((f, t) => f ++ t.overrideMappings) ++ test.overrideMappings
        val allFixtures = parents.foldLeft(Map[String,Prototype[Target]]())((f, t) => f ++ t.fixtures) ++ test.fixtures
        val allAssertions = parents.foldLeft(Map[String,Prototype[Assertion]]())((f, t) => f ++ t.assertions) ++ test.assertions

        Test(
            test.instanceProperties,
            allEnvironment,
            allTargets,
            allRelationMocks,
            allMappingMocks,
            allFixtures,
            allAssertions
        )
    }
}


final case class Test(
    instanceProperties:Test.Properties,
    environment:Map[String,String] = Map(),
    targets:Seq[TargetIdentifier] = Seq(),

    overrideRelations:Map[String,Prototype[Relation]] = Map(),
    overrideMappings:Map[String,Prototype[Mapping]] = Map(),
    fixtures:Map[String,Prototype[Target]] = Map(),
    assertions:Map[String,Prototype[Assertion]] = Map()
) extends AbstractInstance {
    override def category: Category = Category.TEST
    override def kind : String = "test"

   /**
     * Returns an identifier for this test
     * @return
     */
    def identifier : TestIdentifier = TestIdentifier(name, project.map(_.name))

    /**
     * Returns a description of the test
     * @return
     */
    def description : Option[String] = instanceProperties.description

    /**
     * Returns a TestInstance used for state management
     * @return
     */
    def instance : TestInstance = {
        TestInstance(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name
        )
    }
}
