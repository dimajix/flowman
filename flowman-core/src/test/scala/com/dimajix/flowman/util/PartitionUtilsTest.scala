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

package com.dimajix.flowman.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class PartitionUtilsTest extends FlatSpec with Matchers {
    private def concat(partitions:Map[String,Any]) : String = partitions.map{ case(k,v) => k+"="+v }.mkString(",")

    "PartitionUtils" should "expand correctly with flatMap" in {
        val partitions = Map("p1" -> Seq("p1v1", "p1v2"), "p2" -> Seq("p2v1", "p2v2"))
        val result = PartitionUtils.flatMap(partitions,p => Some(concat(p))).toSet
        result should be (Set("p1=p1v1,p2=p2v1","p1=p1v2,p2=p2v1","p1=p1v1,p2=p2v2","p1=p1v2,p2=p2v2"))
    }
    it should "expand correctly with map" in {
        val partitions = Map("p1" -> Seq("p1v1", "p1v2"), "p2" -> Seq("p2v1", "p2v2"))
        val result = PartitionUtils.map(partitions,p => concat(p)).toSet
        result should be (Set("p1=p1v1,p2=p2v1","p1=p1v2,p2=p2v1","p1=p1v1,p2=p2v2","p1=p1v2,p2=p2v2"))
    }
}
