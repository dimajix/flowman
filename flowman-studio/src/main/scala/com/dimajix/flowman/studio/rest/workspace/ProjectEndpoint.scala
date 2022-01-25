/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path

import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.NoSuchProjectException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.storage.Store
import com.dimajix.flowman.storage.Workspace
import com.dimajix.flowman.studio.model
import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.service.SessionManager


@Api(value = "workspace", produces = "application/json", consumes = "application/json")
@Path("/workspace/{workspace}/project")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspace", value = "name of workspace", required = true,
        dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class ProjectEndpoint(sessionManager: SessionManager) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(workspace:Workspace) : Route = pathPrefix("project") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listProjects(workspace)
            }
        }
        ~
        pathPrefix(Segment) { project =>
            withProject(workspace, project) { project => (
                pathEndOrSingleSlash {
                    redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                        infoProject(project)
                    }
                }
                ~
                path("run") {
                    parameterMap { params =>
                        val phase = params.get("phase")
                        val force = params.get("force").map(_.toBoolean)
                        val args = params.filter(_._1.startsWith("param.")).map { case(k,v) => k.stripPrefix("param.") -> v }
                        run(workspace, project, phase, args, force)
                    }
                }
            )}
        }
    )}

    @GET
    @Path("/")
    @ApiOperation(value = "Retrieve a list of all projects", nickname = "getProjects", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project information", response = classOf[model.ProjectList])
    ))
    def listProjects(@ApiParam(hidden = true) store:Store): server.Route = {
        get {
            val result = store.listProjects()
            complete(model.ProjectList(result.map(Converter.of)))
        }
    }

    @GET
    @Path("/{project}")
    @ApiOperation(value = "Retrieve general information about a project", nickname = "getProject", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project information", response = classOf[model.Project]),
        new ApiResponse(code = 404, message = "Project not found", response = classOf[model.Project])
    ))
    def infoProject(@ApiParam(hidden = true) project:Project): server.Route = {
        get {
            complete(Converter.of(project))
        }
    }

    @POST
    @Path("/{project}/run")
    @ApiOperation(value = "Run a single project", nickname = "runProject", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "phase", value = "Build phase", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "force", value = "Force processing even if target is not dirty", required = false,
            dataType = "boolean", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project execution result", response = classOf[model.Status])
    ))
    def run(
        @ApiParam(hidden = true) workspace:Workspace,
        @ApiParam(hidden = true) project:Project,
        @ApiParam(hidden = true) phase: Option[String],
        @ApiParam(hidden = true) args: Map[String,String],
        @ApiParam(hidden = true) force: Option[Boolean]
    ): server.Route = {
        post {
            val session = sessionManager.createSession(workspace, project)
            try {
                val job = session.getJob("main")
                val runner = session.runner
                val phases = Lifecycle.ofPhase(phase.map(Phase.ofString).getOrElse(Phase.BUILD))
                val jargs = job.arguments(args)
                runner.executeJob(job, phases, jargs, force = force.getOrElse(false))
            }
            finally {
                session.close()
            }
            complete(model.Status("success"))
        }
    }

    private def withProject(workspace:Workspace, projectName:String)(fn:Project => server.Route) : server.Route= {
        Try {
            workspace.loadProject(projectName)
        }
        match {
            case Success(result) =>
                fn(result)
            case Failure(_:NoSuchProjectException) =>
                complete(StatusCodes.NotFound -> s"Project '$projectName' not found")
            case Failure(ex) =>
                throw ex
        }

    }
}
