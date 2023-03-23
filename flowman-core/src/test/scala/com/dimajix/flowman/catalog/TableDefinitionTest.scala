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

package com.dimajix.flowman.catalog

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType


class TableDefinitionTest extends AnyFlatSpec with Matchers {
    "TableDefinition" should "return correct columns without partitions" in {
        val table = TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)))

        table.columns should be (Seq(Field("f1", StringType), Field("f2", IntegerType)))
        table.dataColumns should be (Seq(Field("f1", StringType), Field("f2", IntegerType)))
        table.partitionColumns should be (Seq.empty)
    }

    it should "return correct columns with partitions" in {
        val table = TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1"))

        table.columns should be (Seq(Field("f1", StringType), Field("f2", IntegerType)))
        table.dataColumns should be (Seq(Field("f2", IntegerType)))
        table.partitionColumns should be (Seq(Field("f1", StringType)))
    }
}
