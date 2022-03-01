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

package com.dimajix.flowman.spec.test

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import com.dimajix.flowman.common.ParserUtils.splitSettings
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.spec.NamedSpec
import com.dimajix.flowman.spec.assertion.AssertionSpec
import com.dimajix.flowman.spec.mapping.MappingSpec
import com.dimajix.flowman.spec.relation.RelationSpec
import com.dimajix.flowman.spec.target.TargetSpec


object TestSpec {
    final class NameResolver extends NamedSpec.NameResolver[TestSpec]
}


class TestSpec extends NamedSpec[Test] {
    @JsonProperty(value="extends") private var parents:Seq[String] = Seq()
    @JsonProperty(value="description") private var description:Option[String] = None
    @JsonProperty(value="environment") private var environment: Seq[String] = Seq()
    @JsonProperty(value="targets") private var targets: Seq[String] = Seq()
    @JsonDeserialize(converter=classOf[TargetSpec.NameResolver])
    @JsonProperty(value="fixtures") private var fixtures: Map[String,TargetSpec] = Map()
    @JsonDeserialize(converter=classOf[MappingSpec.NameResolver])
    @JsonProperty(value="overrideMappings") private var overrideMappings: Map[String,MappingSpec] = Map()
    @JsonDeserialize(converter=classOf[RelationSpec.NameResolver])
    @JsonProperty(value="overrideRelations") private var overrideRelations: Map[String,RelationSpec] = Map()
    @JsonDeserialize(converter=classOf[AssertionSpec.NameResolver])
    @JsonProperty(value="assertions") private var assertions: Map[String,AssertionSpec] = Map()

    override def instantiate(context: Context): Test = {
        require(context != null)

        val parents = this.parents.map(job => context.getTest(TestIdentifier(job)))
        val test = Test(
            instanceProperties(context),
            environment = splitSettings(environment).toMap,
            targets = targets.map(context.evaluate).map(TargetIdentifier.parse),
            fixtures = fixtures,
            overrideMappings = overrideMappings,
            overrideRelations = overrideRelations,
            assertions = assertions
        )

        Test.merge(test, parents)
    }

    /**
     * Returns a set of common properties
     *
     * @param context
     * @return
     */
    override protected def instanceProperties(context: Context): Test.Properties = {
        require(context != null)
        val name = context.evaluate(this.name)
        Test.Properties(
            context,
            metadata.map(_.instantiate(context, name, Category.TEST, "test")).getOrElse(Metadata(context, name, Category.TEST, "test")),
            description.map(context.evaluate)
        )
    }
}
