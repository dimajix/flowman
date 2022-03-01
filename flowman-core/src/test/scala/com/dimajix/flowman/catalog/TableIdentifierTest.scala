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

package com.dimajix.flowman.catalog

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TableIdentifierTest extends AnyFlatSpec with Matchers {
    "The TableIdentifier" should "work without a namespace" in {
        val id = TableIdentifier("some_table")
        id.toString should be ("`some_table`")
        id.quotedString should be ("`some_table`")
        id.unquotedString should be ("some_table")
        id.database should be (None)
        id.quotedDatabase should be (None)
        id.unquotedDatabase should be (None)
        id.toSpark should be (org.apache.spark.sql.catalyst.TableIdentifier("some_table", None))
    }

    it should "work with a single namespace" in {
        val id = TableIdentifier("some_table", Some("db"))
        id.toString should be ("`db`.`some_table`")
        id.quotedString should be ("`db`.`some_table`")
        id.unquotedString should be ("db.some_table")
        id.database should be (Some("db"))
        id.quotedDatabase should be (Some("`db`"))
        id.unquotedDatabase should be (Some("db"))
        id.toSpark should be (org.apache.spark.sql.catalyst.TableIdentifier("some_table", Some("db")))
    }

    it should "work with a nested namespace" in {
        val id = TableIdentifier("some_table", Seq("db","ns"))
        id.toString should be ("`db`.`ns`.`some_table`")
        id.quotedString should be ("`db`.`ns`.`some_table`")
        id.unquotedString should be ("db.ns.some_table")
        id.database should be (Some("db.ns"))
        id.quotedDatabase should be (Some("`db`.`ns`"))
        id.unquotedDatabase should be (Some("db.ns"))
        id.toSpark should be (org.apache.spark.sql.catalyst.TableIdentifier("some_table", Some("db.ns")))
    }
}
