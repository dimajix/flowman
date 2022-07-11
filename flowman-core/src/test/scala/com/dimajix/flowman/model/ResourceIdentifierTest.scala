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

package com.dimajix.flowman.model

import java.io.File

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ResourceIdentifierTest extends AnyFlatSpec with Matchers {
    "A ResourceIdentifier" should "support basic methods" in {
        val id = ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz"))
        id.category should be ("hiveTablePartition")
        id.name should be ("some_table")
        id.partition should be (Map("p1" -> "xyz"))
    }

    it should "support 'contains' with partitions" in {
        val id = ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz"))
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz"))) should be (true)
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "zyx"))) should be (false)
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map())) should be (false)
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz", "p2" -> "abc"))) should be (true)
    }

    it should "support changing partitions" in {
        val id = ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz"))
        id.partition should be (Map("p1" -> "xyz"))

        val id2 = id.withPartition(Map("p2" -> "abc"))
        id2 should be (ResourceIdentifier.ofHivePartition("some_table", None, Map("p2" -> "abc")))
        id2.partition should be (Map("p2" -> "abc"))
    }

    it should "enumerate all super-partitions" in {
        val id = ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz", "p2" -> "abc"))
        id.partition should be (Map("p1" -> "xyz", "p2" -> "abc"))
        id.explodePartitions() should be (Seq(
            ResourceIdentifier.ofHivePartition("some_table", None, Map()),
            ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz")),
            ResourceIdentifier.ofHivePartition("some_table", None, Map("p2" -> "abc")),
            ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz", "p2" -> "abc"))
        ))
    }

    it should "support files" in {
        val id = ResourceIdentifier.ofFile(new Path("/path/?/with/wildcard"))
        id should be (GlobbingResourceIdentifier("file", "/path/?/with/wildcard"))
        id.isEmpty should be (false)
        id.nonEmpty should be (true)
        id.category should be ("file")
        id.name should be ("/path/?/with/wildcard")
        id.partition should be (Map())

        ResourceIdentifier.ofFile(new Path("file:/path/?/with/wildcard"))  should be (GlobbingResourceIdentifier("file", "file:/path/?/with/wildcard"))
    }

    it should "support local files" in {
        if (System.getProperty("os.name").startsWith("Windows")) {
            ResourceIdentifier.ofLocal(new Path("C:/path/?/with/wildcard")) should be(GlobbingResourceIdentifier("local", "/C:/path/?/with/wildcard"))
            ResourceIdentifier.ofLocal(new Path("file:/C:/path/?/with/wildcard")) should be(GlobbingResourceIdentifier("local", "/C:/path/?/with/wildcard"))
            ResourceIdentifier.ofLocal(new File("/C:/path/?/with/wildcard")) should be(GlobbingResourceIdentifier("local", "/C:/path/?/with/wildcard"))
        }
        else {
            ResourceIdentifier.ofLocal(new Path("/path/?/with/wildcard")) should be(GlobbingResourceIdentifier("local", "/path/?/with/wildcard"))
            ResourceIdentifier.ofLocal(new Path("file:/path/?/with/wildcard")) should be(GlobbingResourceIdentifier("local", "/path/?/with/wildcard"))
            ResourceIdentifier.ofLocal(new File("/path/?/with/wildcard")) should be(GlobbingResourceIdentifier("local", "/path/?/with/wildcard"))
        }
    }

    it should "support Hive databases" in {
        val id =  ResourceIdentifier.ofHiveDatabase("db")
        id should be (RegexResourceIdentifier("hiveDatabase", "db", caseSensitive=false))
        id.isEmpty should be (false)
        id.nonEmpty should be (true)
        id.category should be ("hiveDatabase")
        id.name should be ("db")
    }

    it should "support Hive tables" in {
        ResourceIdentifier.ofHiveTable("table") should be (RegexResourceIdentifier("hiveTable", "table", caseSensitive=false))
        ResourceIdentifier.ofHiveTable("table", None) should be (RegexResourceIdentifier("hiveTable", "table", caseSensitive=false))
        ResourceIdentifier.ofHiveTable("table", Some("db")) should be (RegexResourceIdentifier("hiveTable", "db.table", caseSensitive=false))
    }

    it should "support Hive table partitions" in {
        ResourceIdentifier.ofHivePartition("table", Some("db"), Map("p1" -> "v1", "p2" -> 2)) should be (RegexResourceIdentifier("hiveTablePartition", "db.table", Map("p1" -> "v1", "p2" -> "2"), caseSensitive=false))
        ResourceIdentifier.ofHivePartition("table", None, Map("p1" -> "v1", "p2" -> 2)) should be (RegexResourceIdentifier("hiveTablePartition", "table", Map("p1" -> "v1", "p2" -> "2"), caseSensitive=false))
    }

    it should "support JDBC databases" in {
        ResourceIdentifier.ofJdbcDatabase("db") should be (RegexResourceIdentifier("jdbcDatabase", "db"))
    }

    it should "support JDBC tables" in {
        ResourceIdentifier.ofJdbcTable("table", None) should be (RegexResourceIdentifier("jdbcTable", "table"))
        ResourceIdentifier.ofJdbcTable("table", Some("db")) should be (RegexResourceIdentifier("jdbcTable", "db.table"))
    }

    it should "support JDBC table partitions" in {
        ResourceIdentifier.ofJdbcTablePartition("table", Some("db"), Map("p1" -> "v1", "p2" -> 2)) should be (RegexResourceIdentifier("jdbcTablePartition", "db.table", Map("p1" -> "v1", "p2" -> "2")))
        ResourceIdentifier.ofJdbcTablePartition("table", None, Map("p1" -> "v1", "p2" -> 2)) should be (RegexResourceIdentifier("jdbcTablePartition", "table", Map("p1" -> "v1", "p2" -> "2")))
    }

    "A GlobbingResourceIdentifier" should "provide correct contains semantics" in {
        val parent = ResourceIdentifier.ofFile(new Path("/some/parent/path"))
        parent should be (ResourceIdentifier.ofFile(new Path("/some/parent/path")))
        parent should be (ResourceIdentifier.ofFile(new Path("/some/parent/path/")))

        parent.contains(parent) should be (true)
        parent.contains(ResourceIdentifier.ofFile(new Path("/some/parent/path"))) should be (true)
        parent.contains(ResourceIdentifier.ofFile(new Path("/some/parent/path/"))) should be (true)
        parent.contains(ResourceIdentifier.ofFile(new Path("/some/parent"))) should be (false)
        parent.contains(ResourceIdentifier.ofFile(new Path("/some/parent/"))) should be (false)
        parent.contains(ResourceIdentifier.ofFile(new Path("/some/parent/path/subdir"))) should be (true)
        parent.contains(ResourceIdentifier.ofFile(new Path("/some/parent/path/subdir/"))) should be (true)
    }

    it should "support 'intersects'" in {
        val parent = ResourceIdentifier.ofFile(new Path("/some/parent/path"))

        parent.intersects(parent) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/path"))) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/path/"))) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent"))) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/"))) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/path/subdir"))) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/path/subdir/"))) should be (true)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/unrelated"))) should be (false)
        parent.intersects(ResourceIdentifier.ofFile(new Path("/some/parent/unrelated/"))) should be (false)
    }

    it should "support 'contains' with multi char wildcards" in {
        val id = ResourceIdentifier.ofFile(new Path("/path/*/with/wildcard"))
        id.contains(ResourceIdentifier.ofFile(new Path("/path/*/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/?/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/1/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/1/withx/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/123/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path//with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/*/xyz/with/wildcard"))) should be (false)
        //id.contains(ResourceIdentifier.ofFile(new Path("/path//xyz/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/123/xyz/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/*/*/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/*/with"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/?/with"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/1/with"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/*/with/wildcard_x"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/?/with/wildcard_x"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/1/with/wildcard_x"))) should be (false)
    }

    it should "support 'contain' with wildcards is partial directories" in {
        val id = ResourceIdentifier.ofFile(new Path("/path/topic.*.dev/with/wildcard"))
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.*.dev/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.?.dev/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.1.dev/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic..dev/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.1dev/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic1.dev/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.1.dev/withx/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.*.dev/with"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.?.dev/with"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.1.dev/with"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.*.dev/with/wildcard_x"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.?.dev/with/wildcard_x"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/topic.1.dev/with/wildcard_x"))) should be (false)
    }

    it should "support 'contains' with single char wildcards" in {
        val id = ResourceIdentifier.ofFile(new Path("/path/?/with/wildcard"))
        id.contains(ResourceIdentifier.ofFile(new Path("/path/?/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/*/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/x/with/wildcard"))) should be (true)
        id.contains(ResourceIdentifier.ofFile(new Path("/path//with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/xy/with/wildcard"))) should be (false)
        id.contains(ResourceIdentifier.ofFile(new Path("/path/x/y/with/wildcard"))) should be (false)
    }

    it should "support windows paths" in {
        val id = ResourceIdentifier.ofFile(new Path("C:/Temp/1572861822921-0/topic=publish.Card.*.dev/processing_date=2019-03-20"))
        id.contains(ResourceIdentifier.ofFile(new Path("C:/Temp/1572861822921-0/topic=publish.Card.test.dev/processing_date=2019-03-20"))) should be (true)
    }

    "A RegexResourceIdentifier" should "support character sets in regex" in {
        val id = ResourceIdentifier.ofHiveTable("table_[0-9]+")
        id.contains(ResourceIdentifier.ofHiveTable("table_[0-9]+")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("table_+")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("table_")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("table_0")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("table_01")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("table_0x")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("table_0+")) should be (false)
    }

    it should "support case insensitive comparisons" in {
        val id = ResourceIdentifier.ofHiveTable("table_[0-9]+")
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_[0-9]+")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_+")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_0")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_01")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_0x")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("TABLE_0+")) should be (false)
    }

    it should "support case sensitive comparisons" in {
        val id = ResourceIdentifier.ofJdbcTable("table_[0-9]+")
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_[0-9]+")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_+")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_0")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_01")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_0x")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("TABLE_0+")) should be (false)

        id.contains(ResourceIdentifier.ofJdbcTable("table_[0-9]+")) should be (true)
        id.contains(ResourceIdentifier.ofJdbcTable("table_+")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("table_")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("table_0")) should be (true)
        id.contains(ResourceIdentifier.ofJdbcTable("table_01")) should be (true)
        id.contains(ResourceIdentifier.ofJdbcTable("table_0x")) should be (false)
        id.contains(ResourceIdentifier.ofJdbcTable("table_0+")) should be (false)
    }
}
