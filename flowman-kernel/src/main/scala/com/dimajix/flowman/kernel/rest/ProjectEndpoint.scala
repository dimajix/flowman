/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.kernel.rest

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Route
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path

import com.dimajix.flowman.execution.NoSuchProjectException
import com.dimajix.flowman.kernel.model.Converter
import com.dimajix.flowman.kernel.model.Project
import com.dimajix.flowman.kernel.model.ProjectList
import com.dimajix.flowman.storage.Store


@Api(value = "/project", produces = "application/json", consumes = "application/json")
@Path("/project")
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class ProjectEndpoint(store:Store) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.kernel.model.JsonSupport._

    def routes : Route = pathPrefix("project") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listProjects()
            }
        }
        ~
        pathPrefix(Segment) { project =>
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                    infoProject(project)
                }
            }
        }
    )}

    @Path("/")
    @ApiOperation(value = "Retrieve a list of all projects", nickname = "getProjects", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project information", response = classOf[ProjectList])
    ))
    def listProjects(): server.Route = {
        val result = store.listProjects()
        complete(ProjectList(result.map(Converter.of)))
    }

    @Path("/{project}")
    @ApiOperation(value = "Retrieve general information about a project", nickname = "getProject", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project information", response = classOf[Project]),
        new ApiResponse(code = 404, message = "Project not found", response = classOf[Project])
    ))
    def infoProject(@ApiParam(hidden = true) project:String): server.Route = {
        Try {
            Converter.of(store.loadProject(project))
        }
        match {
            case Success(result) =>
                complete(result)
            case Failure(_:NoSuchProjectException) =>
                complete(StatusCodes.NotFound)
            case Failure(ex) =>
                complete(StatusCodes.InternalServerError)
        }
    }
}
