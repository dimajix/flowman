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
            .map(test => test.targets.toSet)
            .reduceOption((targets, elems) => targets ++ elems)
            .getOrElse(Set())

        val allEnvironment = parentEnvironment ++ test.environment

        val allTargets = parentTargets ++ test.targets
        val allRelationMocks = parents.foldLeft(Map[String,Template[Relation]]())((f,t) => f ++ t.relationOverrides) ++ test.relationOverrides
        val allMappingMocks = parents.foldLeft(Map[String,Template[Mapping]]())((f,t) => f ++ t.mappingOverrides) ++ test.mappingOverrides
        val allFixtures = parents.foldLeft(Map[String,Template[Target]]())((f,t) => f ++ t.fixtures) ++ test.fixtures
        val allAssertions = parents.foldLeft(Map[String,Template[Assertion]]())((f,t) => f ++ t.assertions) ++ test.assertions

        Test(
            test.instanceProperties,
            allEnvironment,
            allTargets.toSeq,
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

    relationOverrides:Map[String,Template[Relation]] = Map(),
    mappingOverrides:Map[String,Template[Mapping]] = Map(),
    fixtures:Map[String,Template[Target]] = Map(),
    assertions:Map[String,Template[Assertion]] = Map()
) extends AbstractInstance {
    override def category: String = "test"
    override def kind : String = "test"

    def identifier : TestIdentifier = TestIdentifier(name, project.map(_.name))

    def description : Option[String] = instanceProperties.description
}
