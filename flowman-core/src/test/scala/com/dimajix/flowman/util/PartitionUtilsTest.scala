package com.dimajix.flowman.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class PartitionUtilsTest extends FlatSpec with Matchers {
    private def concat(partitions:Map[String,String]) : String = partitions.map{ case(k,v) => k+"="+v }.mkString(",")

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
