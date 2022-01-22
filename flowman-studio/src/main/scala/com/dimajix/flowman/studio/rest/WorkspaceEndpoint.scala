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

package com.dimajix.flowman.studio.rest

import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.GET
import javax.ws.rs.Path

import com.dimajix.flowman.storage.Workspace
import com.dimajix.flowman.studio.model.TargetList
import com.dimajix.flowman.studio.model.WorkspaceList
import com.dimajix.flowman.studio.service.WorkspaceManager


@Api(value = "/workspace", produces = "application/json", consumes = "application/json")
@Path("/workspace")
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class WorkspaceEndpoint(workspaceManager:WorkspaceManager) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    private val projectEndpoint:ProjectEndpoint = new ProjectEndpoint
    private val parcelEndpoint:ParcelEndpoint = new ParcelEndpoint

    def routes : server.Route = pathPrefix("workspace") {(
        pathEndOrSingleSlash {(
            listWorkspaces()
            ~
            createWorkspace()
        )}
        ~
        pathPrefix(Segment) { workspace =>
            withWorkspace(workspace) { workspace => (
                pathEndOrSingleSlash {(
                    getWorkspace(workspace)
                    ~
                    deleteWorkspace(workspace)
                )}
                ~
                projectEndpoint.routes(workspace)
                ~
                parcelEndpoint.routes(workspace)
            )}
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all worksapces", nickname = "listWorkspaces", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all targets", response = classOf[WorkspaceList])
    ))
    def listWorkspaces() : server.Route = {
        get {
            val result = WorkspaceList(workspaceManager.list())
            complete(result)
        }
    }
    private def createWorkspace() : server.Route = {
        post {
            ???
        }
    }
    private def getWorkspace(workspace:Workspace) : server.Route = {
        get {
            ???
        }
    }
    private def deleteWorkspace(workspace:Workspace) : server.Route = {
        delete {
            ???
        }
    }

    private def withWorkspace(name:String)(fn:Workspace => server.Route) : server.Route = ???
}
