package com.dimajix.dataflow.spec

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class ModuleTest extends FlatSpec with Matchers {
    "The Module" should "be loadable from a string" in {
        val spec =
            """
              |environment:
              |  - x=y
              |config:
              |  - spark.lala=lolo
            """.stripMargin
        val module = Module.read.string(spec)
        module.environment should contain("x" -> "y")
        module.config should contain("spark.lala" -> "lolo")
    }

}
