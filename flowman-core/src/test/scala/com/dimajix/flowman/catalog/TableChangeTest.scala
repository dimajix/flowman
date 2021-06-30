/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.catalog.TableChange.AddColumn
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType
import com.dimajix.flowman.types.VarcharType


class TableChangeTest extends AnyFlatSpec with Matchers {
    "TableChange.requiresMigration" should "accept same schemas in strict mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (false)
    }

    it should "not accept dropped columns in strict mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            StructType(Seq(Field("f1", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "not accept added columns in strict mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType))),
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "not accept changed data types in strict mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", IntegerType), Field("f2", StringType))),
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            StructType(Seq(Field("f1", IntegerType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", LongType), Field("f2", StringType))),
            StructType(Seq(Field("f1", IntegerType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", VarcharType(10)), Field("f2", StringType))),
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "not accept changed nullability in strict mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, true))),
            StructType(Seq(Field("f1", StringType, false))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, false))),
            StructType(Seq(Field("f1", StringType, true))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "accept changed comments in strict mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = Some("lala")))),
            StructType(Seq(Field("f1", StringType, description = Some("lala")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = Some("lala")))),
            StructType(Seq(Field("f1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = None))),
            StructType(Seq(Field("f1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = Some("lala")))),
            StructType(Seq(Field("f1", StringType, description = None))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "accept same schemas in relaxed mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            StructType(Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "handle data type in relaxed mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", LongType), Field("f2", StringType))),
            StructType(Seq(Field("f1", IntegerType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", IntegerType), Field("f2", StringType))),
            StructType(Seq(Field("f1", LongType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "accept removed columns in relaxed mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", LongType), Field("f2", StringType))),
            StructType(Seq(Field("f1", LongType))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "not accept added columns in relaxed mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", LongType))),
            StructType(Seq(Field("f1", LongType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "accept changed comments in relaxed mode" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = Some("lala")))),
            StructType(Seq(Field("F1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = None))),
            StructType(Seq(Field("F1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, description = Some("lala")))),
            StructType(Seq(Field("F1", StringType, description = None))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "handle changed nullability" in {
        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, true), Field("f2", StringType))),
            StructType(Seq(Field("F1", StringType, false), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            StructType(Seq(Field("f1", StringType, false), Field("f2", StringType))),
            StructType(Seq(Field("F1", StringType, true), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    "TableChange.migrate" should "work in strict mode" in {
        val changes = TableChange.migrate(
            StructType(Seq(
                Field("f1", StringType, true),
                Field("f2", LongType),
                Field("f3", StringType),
                Field("f4", StringType),
                Field("f6", StringType, false)
            )),
            StructType(Seq(
                Field("F1", StringType, false),
                Field("F2", StringType),
                Field("F3", LongType),
                Field("F5", StringType),
                Field("F6", StringType, true),
            )),
            MigrationPolicy.STRICT
        )

        changes should be (Seq(
            DropColumn("f4"),
            UpdateColumnNullability("f1", false),
            UpdateColumnType("f2", StringType),
            UpdateColumnType("f3", LongType),
            AddColumn(Field("F5", StringType)),
            UpdateColumnNullability("f6", true),
        ))
    }

    it should "work in relaxed mode" in {
        val changes = TableChange.migrate(
            StructType(Seq(
                Field("f1", StringType, true),
                Field("f2", LongType),
                Field("f3", StringType),
                Field("f4", StringType),
                Field("f6", StringType, false)
            )),
            StructType(Seq(
                Field("F1", StringType, false),
                Field("F2", StringType),
                Field("F3", LongType),
                Field("F5", StringType),
                Field("F6", StringType, true)
            )),
            MigrationPolicy.RELAXED
        )

        changes should be (Seq(
            UpdateColumnType("f2", StringType),
            AddColumn(Field("F5", StringType)),
            UpdateColumnNullability("f6", true)
        ))
    }
}
