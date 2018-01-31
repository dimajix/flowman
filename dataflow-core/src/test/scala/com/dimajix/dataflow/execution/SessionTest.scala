package com.dimajix.dataflow.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SessionTest extends FlatSpec with Matchers {
    "A Session" should "be buildable" in {
        val session = Session.builder()
            .build
        session should not be (null)
    }

    it should "contain a valid context" in {
        val session = Session.builder()
            .build
        session.context should not be (null)
    }
}
