package com.dimajix.flowman.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class IdentifierTest extends FlatSpec with Matchers {
    "The TableIdentifier" should "be parsed correctly" in {
        TableIdentifier.parse("lala") should be (new TableIdentifier("lala", None))
        TableIdentifier.parse("project/lala") should be (new TableIdentifier("lala", Some("project")))
        TableIdentifier.parse("p1/p2/lala") should be (new TableIdentifier("lala", Some("p1/p2")))
    }

    it should "be stringified corectly" in {
        new TableIdentifier("lala", None).toString should be ("lala")
        new TableIdentifier("lala", Some("project")).toString should be ("project/lala")
        new TableIdentifier("lala", Some("p1/p2")).toString should be ("p1/p2/lala")
    }
}
