/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.target

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.ScopeContext
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.IdentifierRelationReference
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.types.FieldValue
import com.dimajix.spark.testing.LocalSparkSession


class DropTargetTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The DropTarget" should "be parseable" in {
        val spec =
            """
              |kind: drop
              |relation: some_relation
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val context = session.context

        val targetSpec = ObjectMapper.parse[TargetSpec](spec)
        val target = targetSpec.instantiate(context).asInstanceOf[DropTarget]

        target.relation should be (IdentifierRelationReference(context, "some_relation"))
    }

    it should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution

        val relationTemplate = mock[Prototype[Relation]]
        val relation = mock[Relation]
        val context = ScopeContext.builder(session.context)
            .withRelations(Map("some_relation" -> relationTemplate))
            .build()
        val target = DropTarget(
            context,
            RelationIdentifier("some_relation")
        )

        (relationTemplate.instantiate _).expects(*, None).returns(relation)

        target.phases should be (Set(Phase.CREATE, Phase.VERIFY, Phase.DESTROY))

        target.provides(Phase.VALIDATE) should be (Set())
        target.provides(Phase.CREATE) should be (Set())
        target.provides(Phase.BUILD) should be (Set())
        target.provides(Phase.VERIFY) should be (Set())
        target.provides(Phase.DESTROY) should be (Set())

        target.requires(Phase.VALIDATE) should be (Set())
        (relation.requires _).expects().returns(Set(ResourceIdentifier.ofHiveDatabase("db")))
        (relation.provides _).expects().returns(Set(ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.CREATE) should be (Set(ResourceIdentifier.ofHiveDatabase("db"), ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set())
        (relation.requires _).expects().returns(Set(ResourceIdentifier.ofHiveDatabase("db")))
        (relation.provides _).expects().returns(Set(ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.DESTROY) should be (Set(ResourceIdentifier.ofHiveDatabase("db"), ResourceIdentifier.ofHiveTable("some_table")))

        (relation.exists _).expects(execution).returns(Yes)
        target.dirty(execution, Phase.CREATE) should be (Yes)
        target.dirty(execution, Phase.VERIFY) should be (Yes)
        (relation.exists _).expects(execution).returns(Yes)
        target.dirty(execution, Phase.DESTROY) should be (Yes)

        (relation.exists _).expects(execution).returns(Yes)
        target.execute(execution, Phase.VERIFY).exception.get shouldBe a[VerificationFailedException]

        (relation.destroy _).expects(execution, true)
        target.execute(execution, Phase.CREATE).status should be (Status.SUCCESS)

        (relation.exists _).expects(execution).returns(Yes)
        target.execute(execution, Phase.VERIFY).status should be (Status.FAILED)

        (relation.exists _).expects(execution).returns(No)
        target.execute(execution, Phase.VERIFY).status should be (Status.SUCCESS)

        (relation.exists _).expects(execution).returns(No)
        target.dirty(execution, Phase.CREATE) should be (No)

        (relation.exists _).expects(execution).returns(No)
        target.dirty(execution, Phase.DESTROY) should be (No)
    }
}
