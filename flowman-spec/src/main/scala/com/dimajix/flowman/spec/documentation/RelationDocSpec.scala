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

import com.dimajix.flowman.documentation.RelationDoc
import com.dimajix.flowman.documentation.SchemaDoc
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.Spec


class RelationDocSpec extends Spec[RelationDoc] {
    @JsonProperty(value="description", required=false) private var description:Option[String] = None
    @JsonProperty(value="columns", required=false) private var columns:Seq[ColumnDocSpec] = Seq()
    @JsonProperty(value="tests", required=false) private var tests:Seq[SchemaTestSpec] = Seq()

    override def instantiate(context: Context): RelationDoc = {
        val doc = RelationDoc(
            None,
            RelationIdentifier.empty,
            context.evaluate(description),
            None,
            Seq(),
            Seq(),
            Map()
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
