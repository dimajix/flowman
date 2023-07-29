/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.documentation

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StringType


class ProjectDocTest extends AnyFlatSpec with MockFactory with Matchers {
    "A ProjectDoc" should "support resolving" in {
        val project = ProjectDoc(
            name = "project"
        )
        val projectRef = project.reference
        val map0 = mock[Mapping]
        (map0.identifier _).expects().atLeastOnce().returns(MappingIdentifier("project/m1"))
        (map0.kind _).expects().atLeastOnce().returns("sql")
        val mapping = MappingDoc(
            parent = Some(projectRef),
            mapping = Some(map0)
        )
        val mappingRef = mapping.reference
        val output = MappingOutputDoc(
            parent = Some(mappingRef),
            identifier = MappingOutputIdentifier("project/m1:main")
        )
        val outputRef = output.reference
        val schema = SchemaDoc(
            parent = Some(outputRef)
        )
        val schemaRef = schema.reference
        val column = ColumnDoc(
            parent = Some(schemaRef),
            field = Field("some_field", StringType, false)
        )
        val columnRef = column.reference

        val finalSchema = schema.copy(columns = Seq(column))
        val finalOutput = output.copy(schema = Some(finalSchema))
        val finalMapping = mapping.copy(outputs = Seq(finalOutput))
        val finalProject = project.copy(mappings = Seq(finalMapping))

        finalProject.resolve(projectRef) should be (Some(finalProject))
        finalProject.resolve(mappingRef) should be (Some(finalMapping))
        finalProject.resolve(outputRef) should be (Some(finalOutput))
        finalProject.resolve(schemaRef) should be (Some(finalSchema))
        finalProject.resolve(columnRef) should be (Some(column))
    }
}
