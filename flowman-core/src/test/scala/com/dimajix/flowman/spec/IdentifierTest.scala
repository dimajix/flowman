package com.dimajix.flowman.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class IdentifierTest extends FlatSpec with Matchers {
    "The TableIdentifier" should "be parsed correctly" in {
        MappingIdentifier.parse("lala") should be (new MappingIdentifier("lala", None))
        MappingIdentifier.parse("project/lala") should be (new MappingIdentifier("lala", Some("project")))
        MappingIdentifier.parse("p1/p2/lala") should be (new MappingIdentifier("lala", Some("p1/p2")))
    }

    it should "be stringified corectly" in {
        new MappingIdentifier("lala", None).toString should be ("lala")
        new MappingIdentifier("lala", Some("project")).toString should be ("project/lala")
        new MappingIdentifier("lala", Some("p1/p2")).toString should be ("p1/p2/lala")
    }
}
