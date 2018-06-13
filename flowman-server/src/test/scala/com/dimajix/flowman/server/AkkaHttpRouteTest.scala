package com.dimajix.flowman.server

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.FlatSpec
import org.scalatest.Matchers

class AkkaHttpRouteTest extends FlatSpec with Matchers with ScalatestRouteTest {

    "Routes" should "work" in {
        val route = path("foo") {
            complete("/foo")
        } ~ path("foo" / "bar") {
            complete("/foo/bar")
        } ~ pathPrefix( "api") {
            complete("/api")
        }

        Get("/foo") ~> route ~> check {
            responseAs[String]  shouldEqual "/foo"
        }
        Get("/foo/") ~> route ~> check {
            handled shouldBe false
        }
        Get("/foo/bar") ~> route ~> check {
            responseAs[String]  shouldEqual "/foo/bar"
        }
        Get("/api") ~> route ~> check {
            responseAs[String]  shouldEqual "/api"
        }
        Get("/api/bar") ~> route ~> check {
            responseAs[String]  shouldEqual "/api"
        }
    }
}
