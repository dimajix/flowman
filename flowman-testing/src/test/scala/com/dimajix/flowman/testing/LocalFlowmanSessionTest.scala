package com.dimajix.flowman.testing

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class LocalFlowmanSessionTest extends FlatSpec with Matchers with LocalFlowmanSession {
    "The Flowman session" should "not be null" in {
        spark should not be (null)
        session should not be (null)
    }
}
