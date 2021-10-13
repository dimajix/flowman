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
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


class TruncateTargetTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The TruncateTarget" should "be parseable" in {
        val spec =
            """
              |kind: truncate
              |relation: some_relation
              |partitions:
              |  p1: "1234"
              |  p2:
              |    start: "a"
              |    end: "x"
              |""".stripMargin

        val session = Session.builder().disableSpark().build()
        val context = session.context

        val targetSpec = ObjectMapper.parse[TargetSpec](spec)
        val target = targetSpec.instantiate(context).asInstanceOf[TruncateTarget]

        target.relation should be (RelationIdentifier("some_relation"))
        target.partitions should be (Map(
            "p1" -> SingleValue("1234"),
            "p2" -> RangeValue("a", "x")
        ))
    }

    it should "work with partitioned data" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution

        val relationTemplate = mock[Prototype[Relation]]
        val relation = mock[Relation]
        val context = ScopeContext.builder(session.context)
            .withRelations(Map("some_relation" -> relationTemplate))
            .build()
        val target = TruncateTarget(
            Target.Properties(context),
            RelationIdentifier("some_relation"),
            Map(
                "p1" -> SingleValue("1234"),
                "p2" -> RangeValue("1", "3")
            )
        )

        (relationTemplate.instantiate _).expects(*).returns(relation)

        target.phases should be (Set(Phase.BUILD, Phase.VERIFY))

        (relation.provides _).expects().returns(Set(ResourceIdentifier.ofHiveTable("some_table")))
        (relation.resources _).expects(Map("p1" -> SingleValue("1234"),"p2" -> RangeValue("1", "3"))).returns(Set(
            ResourceIdentifier.ofHivePartition("some_table", Some("db"), Map("p1" -> "1234", "p2" -> "1")),
            ResourceIdentifier.ofHivePartition("some_table", Some("db"), Map("p1" -> "1234", "p2" -> "2"))
        ))

        target.provides(Phase.VALIDATE) should be (Set())
        target.provides(Phase.CREATE) should be (Set())
        target.provides(Phase.BUILD) should be (Set(
            ResourceIdentifier.ofHiveTable("some_table"),
            ResourceIdentifier.ofHivePartition("some_table", Some("db"), Map("p1" -> "1234", "p2" -> "1")),
            ResourceIdentifier.ofHivePartition("some_table", Some("db"), Map("p1" -> "1234", "p2" -> "2"))
        ))
        target.provides(Phase.VERIFY) should be (Set())
        target.provides(Phase.DESTROY) should be (Set())

        (relation.requires _).expects().returns(Set(ResourceIdentifier.ofHiveDatabase("db")))
        (relation.provides _).expects().returns(Set(ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.VALIDATE) should be (Set())
        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set(ResourceIdentifier.ofHiveDatabase("db"), ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.VERIFY) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        (relation.partitions _).expects().returns(Seq(PartitionField("p1", StringType), PartitionField("p2", IntegerType)))
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("1"))).returns(Yes)
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("2"))).returns(No)
        target.dirty(execution, Phase.BUILD) should be (Yes)
        target.dirty(execution, Phase.VERIFY) should be (Yes)

        (relation.partitions _).expects().returns(Seq(PartitionField("p1", StringType), PartitionField("p2", IntegerType)))
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("1"))).returns(No)
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("2"))).returns(Yes)
        target.execute(execution, Phase.VERIFY).exception.get shouldBe a[VerificationFailedException]

        (relation.truncate _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> RangeValue("1", "3")))
        target.execute(execution, Phase.BUILD).status should be (Status.SUCCESS)

        (relation.partitions _).expects().returns(Seq(PartitionField("p1", StringType), PartitionField("p2", IntegerType)))
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("1"))).returns(No)
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("2"))).returns(No)
        target.execute(execution, Phase.VERIFY).status should be (Status.SUCCESS)

        (relation.partitions _).expects().returns(Seq(PartitionField("p1", StringType), PartitionField("p2", IntegerType)))
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("1"))).returns(No)
        (relation.loaded _).expects(execution, Map("p1" -> SingleValue("1234"),"p2" -> SingleValue("2"))).returns(No)
        target.dirty(execution, Phase.BUILD) should be (No)
    }

    it should "work with unpartitioned data" in {
        val session = Session.builder().withSparkSession(spark).build()
        val execution = session.execution

        val relationTemplate = mock[Prototype[Relation]]
        val relation = mock[Relation]
        val context = ScopeContext.builder(session.context)
            .withRelations(Map("some_relation" -> relationTemplate))
            .build()
        val target = TruncateTarget(
            Target.Properties(context),
            RelationIdentifier("some_relation")
        )

        (relationTemplate.instantiate _).expects(*).returns(relation)

        target.phases should be (Set(Phase.BUILD, Phase.VERIFY))

        (relation.provides _).expects().returns(Set(ResourceIdentifier.ofHiveTable("some_table")))
        (relation.resources _).expects(Map.empty[String,FieldValue]).returns(Set(ResourceIdentifier.ofHiveTable("some_table")))

        target.provides(Phase.VALIDATE) should be (Set())
        target.provides(Phase.CREATE) should be (Set())
        target.provides(Phase.BUILD) should be (Set(ResourceIdentifier.ofHiveTable("some_table")))
        target.provides(Phase.VERIFY) should be (Set())
        target.provides(Phase.DESTROY) should be (Set())

        (relation.requires _).expects().returns(Set(ResourceIdentifier.ofHiveDatabase("db")))
        (relation.provides _).expects().returns(Set(ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.VALIDATE) should be (Set())
        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set(ResourceIdentifier.ofHiveDatabase("db"), ResourceIdentifier.ofHiveTable("some_table")))
        target.requires(Phase.VERIFY) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        (relation.loaded _).expects(execution, Map.empty[String,SingleValue]).returns(Yes)
        target.dirty(execution, Phase.BUILD) should be (Yes)
        target.dirty(execution, Phase.VERIFY) should be (Yes)

        (relation.loaded _).expects(execution, Map.empty[String,SingleValue]).returns(Yes)
        target.execute(execution, Phase.VERIFY).exception.get shouldBe a[VerificationFailedException]

        (relation.truncate _).expects(execution, Map.empty[String,FieldValue])
        target.execute(execution, Phase.BUILD).status should be (Status.SUCCESS)

        (relation.loaded _).expects(execution, Map.empty[String,SingleValue]).returns(No)
        target.execute(execution, Phase.VERIFY).status should be (Status.SUCCESS)

        (relation.loaded _).expects(execution, Map.empty[String,SingleValue]).returns(No)
        target.dirty(execution, Phase.BUILD) should be (No)
    }
}
