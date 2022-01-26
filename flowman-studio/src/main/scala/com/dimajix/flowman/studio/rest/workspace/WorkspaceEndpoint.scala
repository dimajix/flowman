/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.rest.workspace

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
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path

import com.dimajix.flowman.storage.Workspace
import com.dimajix.flowman.studio.model
import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.service.SessionManager
import com.dimajix.flowman.studio.service.WorkspaceManager


@Api(value = "/workspace", produces = "application/json", consumes = "application/json")
@Path("/workspace")
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class WorkspaceEndpoint(workspaceManager:WorkspaceManager, sessionManager: SessionManager) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    private val projectEndpoint:ProjectEndpoint = new ProjectEndpoint(sessionManager)
    private val parcelEndpoint:ParcelEndpoint = new ParcelEndpoint

    def routes : server.Route = pathPrefix("workspace") {(
        pathEndOrSingleSlash {(
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listWorkspaces()
            }
        )}
        ~
        pathPrefix(Segment) { workspace => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {(
                    getWorkspace(workspace)
                    ~
                    createWorkspace(workspace)
                    ~
                    deleteWorkspace(workspace)
                )}
            }
            ~
            withWorkspace(workspace) { workspace => (
                projectEndpoint.routes(workspace)
                ~
                parcelEndpoint.routes(workspace)
            )}
        )}
    )}

    @GET
    @ApiOperation(value = "Return list of all workspaces", nickname = "listWorkspaces", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all workspaces", response = classOf[model.WorkspaceList])
    ))
    def listWorkspaces() : server.Route = {
        get {
            val result = model.WorkspaceList(workspaceManager.list().map(ws => Converter.of(ws)))
            complete(result)
        }
    }

    @POST
    @Path("/{workspace}")
    @ApiOperation(value = "Create a new workspace", nickname = "createWorkspace", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "workspace", value = "name of workspace", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Created new workspace", response = classOf[model.Workspace])
    ))
    def createWorkspace(@ApiParam(hidden = true) workspace:String) : server.Route = {
        post {
            Try {
                workspaceManager.createWorkspace(workspace)
            } match {
                case Success(ws) => complete(StatusCodes.Created -> Converter.of(ws))
                case Failure(_:IllegalArgumentException) => complete(HttpResponse(status = StatusCodes.BadRequest))
                case Failure(ex) => throw ex
            }
        }
    }

    @GET
    @Path("/{workspace}")
    @ApiOperation(value = "Get an existing workspace", nickname = "getWorkspace", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "workspace", value = "name of workspace", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Retrieve information on workspace", response = classOf[model.Workspace])
    ))
    def getWorkspace(@ApiParam(hidden = true) workspace:String) : server.Route = {
        get {
            withWorkspace(workspace) { ws =>
                complete(Converter.of(ws))
            }
        }
    }

    @DELETE
    @Path("/{workspace}")
    @ApiOperation(value = "Delete an existing workspace", nickname = "deleteWorkspace", httpMethod = "DELETE")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Deleted workspace", response = classOf[model.Workspace])
    ))
    def deleteWorkspace(@ApiParam(hidden = true) workspace:String) : server.Route = {
        delete {
            withWorkspace(workspace) { ws =>
                workspaceManager.deleteWorkspace(ws.name)
                complete("")
            }
        }
    }

    private def withWorkspace(name:String)(fn:Workspace => server.Route) : server.Route = {
        Try {
            workspaceManager.getWorkspace(name)
        } match {
            case Success(ws) => fn(ws)
            case Failure(_) => complete(StatusCodes.NotFound -> s"Workspace '$name' not found")
        }
    }
}
