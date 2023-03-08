/*
 * Copyright (C) 2021 The Flowman Authors
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
import com.dimajix.flowman.studio.model.Target
import com.dimajix.flowman.studio.model.TargetList
import com.dimajix.flowman.studio.service.SessionService


@Api(value = "session", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/target")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 404, message = "Session or target not found"),
    new ApiResponse(code = 500, message = "Internal server error")
))
class TargetEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(session:SessionService) : server.Route = pathPrefix("target") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listTargets(session)
            }
        }
        ~
        pathPrefix(Segment) { targetName => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                    getTarget(session, targetName)
                }
            })
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all jobs", nickname = "listTargets", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all targets", response = classOf[TargetList])
    ))
    def listTargets(@ApiParam(hidden = true) session: SessionService) : server.Route = {
        get {
            val result = TargetList(
                session.listTargets()
            )
            complete(result)
        }
    }

    @GET
    @Path("/{target}")
    @ApiOperation(value = "Get target", nickname = "getTarget", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "target", value = "Target Name", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about the target", response = classOf[Target])
    ))
    def getTarget(@ApiParam(hidden = true) session: SessionService, @ApiParam(hidden = true) target:String) : server.Route = {
        get {
            withTarget(session, target) { target =>
                complete(Converter.of(target))
            }
        }
    }


    private def withTarget(session:SessionService, targetName:String)(fn:(model.Target) => server.Route) : server.Route = {
        Try {
            session.getTarget(targetName)
        } match {
            case Success(target) => fn(target)
            case Failure(_) => complete(StatusCodes.NotFound -> s"Target '$targetName' not found")
        }
    }
}
