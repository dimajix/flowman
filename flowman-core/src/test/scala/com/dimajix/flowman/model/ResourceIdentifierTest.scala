/*
 * Copyright 2018 Kaya Kupferschmidt
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

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class ResourceIdentifierTest extends FlatSpec with Matchers {
    "A ResourceIdentifier" should "support 'contains' with partitions" in {
        val id = ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz"))
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz"))) should be (true)
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "zyx"))) should be (false)
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map())) should be (false)
        id.contains(ResourceIdentifier.ofHivePartition("some_table", None, Map("p1" -> "xyz", "p2" -> "abc"))) should be (true)
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

    it should "support character sets" in {
        val id = ResourceIdentifier.ofHiveTable("table_[0-9]+")
        id.contains(ResourceIdentifier.ofHiveTable("table_[0-9]+")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("table_+")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("table_")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("table_0")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("table_01")) should be (true)
        id.contains(ResourceIdentifier.ofHiveTable("table_0x")) should be (false)
        id.contains(ResourceIdentifier.ofHiveTable("table_0+")) should be (false)
    }
}
