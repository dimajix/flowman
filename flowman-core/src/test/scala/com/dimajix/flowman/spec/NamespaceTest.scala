package com.dimajix.flowman.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class NamespaceTest extends FlatSpec with Matchers {
    "A Namespace" should "be creatable from a spec" in {
        val spec =
            """
              |environment:
              | - lala=lolo
            """.stripMargin
        val ns = Namespace.read.string(spec)
        ns.environment.size should be (1)
    }
}
