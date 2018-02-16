package com.dimajix.flowman.spec.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.annotation.RelationType


@RelationType(name = "annotatedRelation")
class AnnotationRelation extends NullRelation { }


class PluginRelationTest extends FlatSpec with Matchers {
    "A plugin" should "be used if present" in {
        val spec =
            """
              |relations:
              |  custom:
              |    type: customRelation
            """.stripMargin
        val module = Module.read.string(spec)
        module.relations.keys should contain("custom")
    }

    "Annotated plugins should" should "be used" in {
        val spec =
            """
              |relations:
              |  custom:
              |    type: annotatedRelation
            """.stripMargin
        val module = Module.read.string(spec)
        module.relations.keys should contain("custom")
    }
}
