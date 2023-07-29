/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.spec.history

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.documentation.ColumnDoc
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.documentation.RelationDoc
import com.dimajix.flowman.documentation.SchemaDoc
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobState
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.spark.testing.LocalTempDir


class JdbcStateRepositoryTest extends AnyFlatSpec with Matchers with LocalTempDir with MockFactory {
    private def newStateStore() = {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection(
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )

        new JdbcStateRepository(connection)
    }

    "The JdbcStateRepository" should "create tables once" in {
        val db = tempDir.toPath.resolve("mydb")
        val connection = JdbcStateStore.Connection(
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )

        val store1 = new JdbcStateRepository(connection)
        store1.create()
        val store2 = new JdbcStateRepository(connection)
        store2.create()
    }

    it should "store project documentation" in {
        val rel0 = mock[Relation]
        (rel0.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/rel0"))
        (rel0.kind _).expects().atLeastOnce().returns("hive")
        val rel1 = mock[Relation]
        (rel1.identifier _).expects().atLeastOnce().returns(RelationIdentifier("project/rel1"))
        (rel1.kind _).expects().atLeastOnce().returns("hive")

        val project = ProjectDoc(
            name = "project",
            version = Some("1.0")
        )
        val projectRef = project.reference

        val relation0 = RelationDoc(
            parent = Some(projectRef),
            relation = Some(rel0)
        )
        val relation0Ref = relation0.reference
        val schema0 = SchemaDoc(
            parent = Some(relation0Ref)
        )
        val schema0Ref = schema0.reference
        val column0 = ColumnDoc(
            parent = Some(schema0Ref),
            field = Field("some_field", StringType, false)
        )

        val relation1 = RelationDoc(
            parent = Some(projectRef),
            relation = Some(rel1)
        )
        val relation1Ref = relation1.reference
        val schema1 = SchemaDoc(
            parent = Some(relation1Ref)
        )
        val schema1Ref = schema1.reference
        val column1 = ColumnDoc(
            parent = Some(schema1Ref),
            field = Field("some_other_field", StringType, false),
            inputs = Seq(column0.reference)
        )
        val column1Ref = column1.reference
        val column1_a = ColumnDoc(
            parent = Some(column1Ref),
            field = Field("child_a", StringType, false),
            inputs = Seq(column0.reference)
        )
        val column1_b = ColumnDoc(
            parent = Some(column1Ref),
            field = Field("child_b", IntegerType, true),
            inputs = Seq(column0.reference)
        )
        val finalColumn1 = column1.copy(
            field = Field("some_other_field", StructType(Seq(Field("child_a", StringType, false), Field("child_b", IntegerType, true)))),
            children = Seq(column1_a, column1_b)
        )

        val finalSchema0 = schema0.copy(columns = Seq(column0))
        val finalRelation0 = relation0.copy(schema = Some(finalSchema0))
        val finalSchema1 = schema1.copy(columns = Seq(finalColumn1))
        val finalRelation1 = relation1.copy(schema = Some(finalSchema1))
        val finalProject = project.copy(relations = Seq(finalRelation0, finalRelation1))

        val store = newStateStore()
        store.create()

        val jobState = JobState(
            "",
            "default",
            "project",
            "1.0",
            "main",
            Phase.BUILD,
            Map.empty,
            Status.SUCCESS
        )
        val dbJobState = store.insertJobState(jobState, Map.empty)
        store.insertJobDocumentation(dbJobState.id, finalProject)

        val expected = finalProject.copy(
            relations = finalProject.relations.map(_.copy(relation = None))
        )
        val actual = store.getJobDocumentation(dbJobState.id)
        actual should be (Some(expected))
    }
}
