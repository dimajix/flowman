/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.rest.session

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.GET
import javax.ws.rs.Path

import com.dimajix.flowman.model
import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model.Test
import com.dimajix.flowman.studio.model.TestList
import com.dimajix.flowman.studio.service.SessionService


@Api(value = "session", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/test")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 404, message = "Session or test not found"),
    new ApiResponse(code = 500, message = "Internal server error")
))
class TestEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(session:SessionService) : server.Route = pathPrefix("test") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listTests(session)
            }
        }
        ~
        pathPrefix(Segment) { testName => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                    getTest(session, testName)
                }
            })
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all jobs", nickname = "listTests", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all tests", response = classOf[TestList])
    ))
    def listTests(@ApiParam(hidden = true) session: SessionService) : server.Route = {
        get {
            val result = TestList(
                session.listTests()
            )
            complete(result)
        }
    }

    @GET
    @Path("/{test}")
    @ApiOperation(value = "Get test", nickname = "getTest", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "test", value = "Test Name", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about the test", response = classOf[Test])
    ))
    def getTest(@ApiParam(hidden = true) session: SessionService, @ApiParam(hidden = true) test:String) : server.Route = {
        get {
            withTest(session, test) { test =>
                complete(Converter.of(test))
            }
        }
    }


    private def withTest(session:SessionService, testName:String)(fn:(model.Test) => server.Route) : server.Route = {
        Try {
            session.getTest(testName)
        } match {
            case Success(test) => fn(test)
            case Failure(_) => complete(StatusCodes.NotFound -> s"Test '$testName' not found")
        }
    }
}
