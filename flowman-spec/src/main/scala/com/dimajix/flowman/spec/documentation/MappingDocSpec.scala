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

package com.dimajix.flowman.spec.documentation

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.documentation.MappingDoc
import com.dimajix.flowman.documentation.MappingOutputDoc
import com.dimajix.flowman.documentation.MappingReference
import com.dimajix.flowman.documentation.SchemaDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.spec.Spec


class MappingOutputDocSpec {
    @JsonProperty(value="description", required=false) private var description:Option[String] = None
    @JsonProperty(value="columns", required=false) private var columns:Seq[ColumnDocSpec] = Seq()
    @JsonProperty(value="tests", required=false) private var tests:Seq[SchemaTestSpec] = Seq()

    def instantiate(context: Context, parent:MappingReference, name:String): MappingOutputDoc = {
        val doc = MappingOutputDoc(
            Some(parent),
            MappingOutputIdentifier.empty.copy(output=name),
            context.evaluate(description),
            None
        )
        val ref = doc.reference

        val schema =
            if (columns.nonEmpty || tests.nonEmpty) {
                val schema = SchemaDoc(
                    Some(ref),
                    None,
                    Seq(),
                    Seq()
                )
                val ref2 = schema.reference
                val cols = columns.map(_.instantiate(context, ref2))
                val tests = this.tests.map(_.instantiate(context, ref2))
                Some(schema.copy(
                    columns=cols,
                    tests=tests
                ))
            }
            else {
                None
            }

        doc.copy(
            schema = schema
        )
    }
}


class MappingDocSpec extends Spec[MappingDoc] {
    @JsonProperty(value="description", required=false) private var description:Option[String] = None
    @JsonProperty(value="outputs", required=false) private var outputs:Map[String,MappingOutputDocSpec] = _
    @JsonProperty(value="columns", required=false) private var columns:Seq[ColumnDocSpec] = Seq()
    @JsonProperty(value="tests", required=false) private var tests:Seq[SchemaTestSpec] = Seq()

    def instantiate(context: Context): MappingDoc = {
        val doc = MappingDoc(
            None,
            MappingIdentifier.empty,
            context.evaluate(description),
            Seq(),
            Seq()
        )
        val ref = doc.reference

        val output =
            if (columns.nonEmpty || tests.nonEmpty) {
                val output = MappingOutputDoc(
                    Some(ref),
                    MappingOutputIdentifier.empty.copy(output="main"),
                    None,
                    None
                )
                val ref2 = output.reference

                val schema = SchemaDoc(
                    Some(ref2),
                    None,
                    Seq(),
                    Seq()
                )
                val ref3 = schema.reference
                val cols = columns.map(_.instantiate(context, ref3))
                val tests = this.tests.map(_.instantiate(context, ref3))
                Some(
                    output.copy(
                        schema = Some(schema.copy(
                            columns=cols,
                            tests=tests
                        ))
                    )
                )
            }
            else {
                None
            }

        val outputs = this.outputs.map { case(name,output) =>
            output.instantiate(context, ref, name)
        } ++ output.toSeq

        doc.copy(outputs=outputs.toSeq)
    }
}
