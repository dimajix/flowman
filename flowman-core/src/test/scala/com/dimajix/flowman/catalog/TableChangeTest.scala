/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import com.dimajix.flowman.catalog.TableChange.CreateIndex
import com.dimajix.flowman.catalog.TableChange.CreatePrimaryKey
import com.dimajix.flowman.catalog.TableChange.DropColumn
import com.dimajix.flowman.catalog.TableChange.DropIndex
import com.dimajix.flowman.catalog.TableChange.DropPrimaryKey
import com.dimajix.flowman.catalog.TableChange.UpdateColumnNullability
import com.dimajix.flowman.catalog.TableChange.UpdateColumnType
import com.dimajix.flowman.catalog.TableChange.UpdatePartitionColumns
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.VarcharType


class TableChangeTest extends AnyFlatSpec with Matchers {
    "TableChange.requiresMigration" should "accept same schemas in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (false)
    }

    it should "not accept dropped columns in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "not accept added columns in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "not accept changed data types in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", IntegerType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", IntegerType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", IntegerType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", VarcharType(10)), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "not accept changed nullability in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, true))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, false))),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, false))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, true))),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    it should "accept changed comments in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lala")))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lala")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lala")))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = None))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lala")))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = None))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "accept same schemas in relaxed mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "handle data type in relaxed mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", IntegerType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", IntegerType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "accept removed columns in relaxed mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "not accept added columns in relaxed mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", LongType), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "accept changed comments in relaxed mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lala")))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = None))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType, description = Some("lolo")))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, description = Some("lala")))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType, description = None))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "handle changed nullability" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, true), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType, false), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType, false), Field("f2", StringType))),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType, true), Field("f2", StringType))),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "handle changed primary key" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), primaryKey=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType), Field("f2", StringType)), primaryKey=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (false)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), primaryKey=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType), Field("f2", StringType)), primaryKey=Seq("f2", "f1")),
            MigrationPolicy.RELAXED
        ) should be (false)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), primaryKey=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType), Field("f2", StringType)), primaryKey=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (true)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), primaryKey=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType), Field("f2", StringType)), primaryKey=Seq()),
            MigrationPolicy.RELAXED
        ) should be (true)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), primaryKey=Seq()),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("F1", StringType), Field("f2", StringType)), primaryKey=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "handle changed index" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            MigrationPolicy.RELAXED
        ) should be (false)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("C1")))),
            MigrationPolicy.RELAXED
        ) should be (false)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("NAME", Seq("C1")))),
            MigrationPolicy.RELAXED
        ) should be (false)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq()),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("NAME", Seq("C1")))),
            MigrationPolicy.RELAXED
        ) should be (true)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq()),
            MigrationPolicy.RELAXED
        ) should be (true)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("other", Seq("C1")))),
            MigrationPolicy.RELAXED
        ) should be (true)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("C1","c2")))),
            MigrationPolicy.RELAXED
        ) should be (true)
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("c2","c1")))),
            TableDefinition(TableIdentifier(""), indexes=Seq(TableIndex("name", Seq("C1","c2")))),
            MigrationPolicy.RELAXED
        ) should be (false)
    }

    it should "handle changed partitions in relaxed mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            MigrationPolicy.RELAXED
        ) should be (true)
    }

    it should "handle changed partitions in strict mode" in {
        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (false)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (true)

        TableChange.requiresMigration(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            MigrationPolicy.STRICT
        ) should be (true)
    }

    "TableChange.migrate" should "work in strict mode" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType, true),
                Field("f2", LongType),
                Field("f3", StringType),
                Field("f4", StringType),
                Field("f6", StringType, false)
            )
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("F1", StringType, false),
                Field("F2", StringType),
                Field("F3", LongType, false),
                Field("F5", StringType),
                Field("F6", StringType, true)
            )
        )
        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)

        changes should be (Seq(
            DropColumn("f4"),
            UpdateColumnNullability("f1", false),
            UpdateColumnType("f2", StringType),
            UpdateColumnType("f3", LongType),
            UpdateColumnNullability("f3", false),
            AddColumn(Field("F5", StringType)),
            UpdateColumnNullability("f6", true)
        ))
    }

    it should "work in relaxed mode" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType, true),
                Field("f2", LongType),
                Field("f3", StringType),
                Field("f4", StringType),
                Field("f6", StringType, false)
            )
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("F1", StringType, false),
                Field("F2", StringType),
                Field("F3", LongType),
                Field("F5", StringType),
                Field("F6", StringType, true)
            )
        )
        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)

        changes should be (Seq(
            UpdateColumnType("f2", StringType),
            AddColumn(Field("F5", StringType)),
            UpdateColumnNullability("f6", true)
        ))
    }

    it should "do nothing on unchanged PK" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq("f1", "f2")
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("F1", StringType),
                Field("F2", LongType),
                Field("F3", StringType)
            ),
            primaryKey = Seq("F2", "f1")
        )
        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq())

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq())
    }

    it should "add PK" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType, nullable=true),
                Field("f2", LongType, nullable=false),
                Field("f3", StringType)
            ),
            primaryKey = Seq()
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("F1", StringType, nullable=false),
                Field("F2", LongType, nullable=false),
                Field("F3", StringType)
            ),
            primaryKey = Seq("f1", "f2")
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq(UpdateColumnNullability("f1",false), CreatePrimaryKey(Seq("f1", "f2"))))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(UpdateColumnNullability("f1",false), CreatePrimaryKey(Seq("f1", "f2"))))
    }

    it should "drop PK" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq("f1", "f2")
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq()
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq(DropPrimaryKey()))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(DropPrimaryKey()))
    }

    it should "drop/add PK" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq("f1", "f2")
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq("f2")
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq(
            DropPrimaryKey(),
            CreatePrimaryKey(Seq("f2"))
        ))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(
            DropPrimaryKey(),
            CreatePrimaryKey(Seq("f2"))
        ))
    }

    it should "drop/add PK on type change" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", StringType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq("f1", "f2")
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns=Seq(
                Field("f1", IntegerType),
                Field("f2", LongType),
                Field("f3", StringType)
            ),
            primaryKey = Seq("f1", "f2")
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes should be (Seq(
            DropPrimaryKey(),
            UpdateColumnType("f1", IntegerType),
            CreatePrimaryKey(Seq("f1", "f2"))
        ))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes2 should be (Seq(
            DropPrimaryKey(),
            UpdateColumnType("f1", IntegerType),
            CreatePrimaryKey(Seq("f1", "f2"))
        ))
    }

    it should "do nothing on an unchanged index" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns = Seq(
                Field("col1", StringType),
                Field("col2", LongType),
                Field("col3", StringType)
            ),
            indexes = Seq(TableIndex("name", Seq("col1", "col2")))
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns = Seq(
                Field("COL1", StringType),
                Field("cOl2", LongType),
                Field("col3", StringType)
            ),
            indexes = Seq(TableIndex("NAME", Seq("col2", "COL1")))
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq.empty)

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq.empty)
    }

    it should "add an index" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            indexes = Seq()
        )
        val newTable = TableDefinition(TableIdentifier(""),
            indexes = Seq(TableIndex("NAME", Seq("col2", "COL1")))
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq(CreateIndex("NAME", Seq("col2", "COL1"), false)))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(CreateIndex("NAME", Seq("col2", "COL1"), false)))
    }

    it should "drop an index" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            indexes = Seq(TableIndex("name", Seq("col1", "col2")))
        )
        val newTable = TableDefinition(TableIdentifier(""),
            indexes = Seq()
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq(DropIndex("name")))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(DropIndex("name")))
    }

    it should "drop/add an index" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            indexes = Seq(TableIndex("name", Seq("col1", "col3")))
        )
        val newTable = TableDefinition(TableIdentifier(""),
            indexes = Seq(TableIndex("NAME", Seq("col2", "COL1")))
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq(DropIndex("name"), CreateIndex("NAME", Seq("col2", "COL1"), false)))

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(DropIndex("name"), CreateIndex("NAME", Seq("col2", "COL1"), false)))
    }

    it should "drop/add an index if data type changes" in {
        val oldTable = TableDefinition(TableIdentifier(""),
            columns = Seq(
                Field("cOl1", StringType),
                Field("CoL2", LongType),
                Field("coL3", StringType)
            ),
            indexes = Seq(TableIndex("name", Seq("col1", "col3")))
        )
        val newTable = TableDefinition(TableIdentifier(""),
            columns = Seq(
                Field("CoL1", IntegerType),
                Field("COl2", LongType),
                Field("COL3", StringType)
            ),
            indexes = Seq(TableIndex("NAME", Seq("col1", "COL3")))
        )

        val changes = TableChange.migrate(oldTable, newTable, MigrationPolicy.RELAXED)
        changes should be (Seq.empty)

        val changes2 = TableChange.migrate(oldTable, newTable, MigrationPolicy.STRICT)
        changes2 should be (Seq(DropIndex("name"), UpdateColumnType("cOl1",IntegerType), CreateIndex("NAME", Seq("col1", "COL3"), false)))
    }

    it should "update all partitions" in {
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (Seq.empty)
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (Seq.empty)

        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (Seq.empty)
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", IntegerType)))
        ))

        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", StringType)))
        ))
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", IntegerType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", StringType)))
        ))

        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", StringType)))
        ))
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", StringType)))
        ))

        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType)), partitionColumnNames=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.RELAXED
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", StringType)))
        ))
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType)), partitionColumnNames=Seq("f1")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            MigrationPolicy.STRICT
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType), Field("f2", StringType)))
        ))

        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            MigrationPolicy.RELAXED
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType)))
        ))
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1")),
            MigrationPolicy.STRICT
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType)))
        ))

        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType)), partitionColumnNames=Seq("f1")),
            MigrationPolicy.RELAXED
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType)))
        ))
        TableChange.migrate(
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType), Field("f2", StringType)), partitionColumnNames=Seq("f1", "f2")),
            TableDefinition(TableIdentifier(""), columns=Seq(Field("f1", StringType)), partitionColumnNames=Seq("f1")),
            MigrationPolicy.STRICT
        ) should be (Seq(
            UpdatePartitionColumns(Seq(Field("f1", StringType)))
        ))
    }
}
