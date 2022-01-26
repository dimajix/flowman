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
import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model.CreateSessionRequest
import com.dimajix.flowman.studio.model.Project
import com.dimajix.flowman.studio.model.Session
import com.dimajix.flowman.studio.model.SessionList
import com.dimajix.flowman.studio.model.Status
import com.dimajix.flowman.studio.service.SessionManager
import com.dimajix.flowman.studio.service.SessionService
import com.dimajix.flowman.studio.service.WorkspaceManager


@Api(value = "session", produces = "application/json", consumes = "application/json")
@Path("/session")
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class SessionEndpoint(workspaceManager:WorkspaceManager, sessionManager:SessionManager) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    private val logger = LoggerFactory.getLogger(classOf[SessionEndpoint])
    private val jobEndpoint:JobEndpoint = new JobEndpoint
    private val mappingEndpoint:MappingEndpoint = new MappingEndpoint
    private val relationEndpoint:RelationEndpoint = new RelationEndpoint
    private val targetEndpoint:TargetEndpoint = new TargetEndpoint
    private val testEndpoint:TestEndpoint = new TestEndpoint

    def routes : server.Route = pathPrefix("session") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {(
                listSessions()
                ~
                createSession()
            )}
        }
        ~
        pathPrefix(Segment) { session =>
            withSession(session) { session =>
            (
                pathEndOrSingleSlash {
                    redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {(
                        getSession(session)
                        ~
                        closeSession(session)
                    )}
                }
                ~
                path("project") {
                    getProject(session)
                }
                ~
                path("reset") {
                    resetSession(session)
                }
                ~
                jobEndpoint.routes(session)
                ~
                mappingEndpoint.routes(session)
                ~
                relationEndpoint.routes(session)
                ~
                targetEndpoint.routes(session)
                ~
                testEndpoint.routes(session)
            )}
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all active sessions", nickname = "listSessions", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about namespace", response = classOf[SessionList])
    ))
    def listSessions() : server.Route = {
        get {
            val result = SessionList(sessionManager.list().map(s => Session(s.id, s.namespace.name, s.project.name)))
            complete(result)
        }
    }

    @POST
    @ApiOperation(value = "Create new session", nickname = "createSession", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "session request", value = "session parameters and project name", required = true,
            dataTypeClass = classOf[CreateSessionRequest], paramType = "body")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 201, message = "Create a new session and opens a project", response = classOf[Session]),
        new ApiResponse(code = 400, message = "Bad request", response = classOf[Session])
    ))
    def createSession() : server.Route = {
        post {
            entity(as[CreateSessionRequest]) { request =>
                Try {
                    val workspace = workspaceManager.getWorkspace(request.workspace)
                    sessionManager.createSession(workspace, request.project)
                }
                match {
                    case Success(session) =>
                        val result = Session(
                            id = session.id,
                            namespace = session.namespace.name,
                            project = session.project.name,
                            config = session.context.config.toMap,
                            environment = session.context.environment.toMap.map(kv => kv._1 -> kv._2.toString)
                        )
                        complete(StatusCodes.Created -> result)
                    case Failure(e) =>
                        logger.warn(s"Cannot load project. Request was $request, error is ${e.getMessage}")
                        complete(HttpResponse(status = StatusCodes.InternalServerError))
                }
            }
        }
    }

    @GET
    @Path("/{session}")
    @ApiOperation(value = "Get session", nickname = "getSession", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Retrieve session information", response = classOf[Session]),
        new ApiResponse(code = 404, message = "Session not found")
    ))
    def getSession(@ApiParam(hidden = true) session:SessionService) : server.Route = {
        get {
            val result = Session(
                id = session.id,
                namespace = session.namespace.name,
                project = session.project.name,
                config = session.context.config.toMap,
                environment = session.context.environment.toMap.map(kv => kv._1 -> kv._2.toString)
            )
            complete(result)
        }
    }

    @POST
    @Path("/{session}/reset")
    @ApiOperation(value = "Reset session", nickname = "resetSession", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Retrieve session information"),
        new ApiResponse(code = 404, message = "Session not found")
    ))
    def resetSession(@ApiParam(hidden = true) session: SessionService) : server.Route = {
        post {
            session.reset()
            complete(Status("success"))
        }
    }

    @DELETE
    @Path("/{session}")
    @ApiOperation(value = "Close session", nickname = "closeSession", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Close current session (and project)"),
        new ApiResponse(code = 404, message = "Session not found")
    ))
    def closeSession(@ApiParam(hidden = true) session:SessionService) : server.Route = {
        delete {
            session.close()
            complete(Status("success"))
        }
    }

    @GET
    @Path("/{session}/project")
    @ApiOperation(value = "Get project", nickname = "getProject", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about the project", response = classOf[Project]),
        new ApiResponse(code = 404, message = "Session not found")
    ))
    def getProject(@ApiParam(hidden = true) session:SessionService) : server.Route = {
        get {
            complete(Converter.of(session.project))
        }
    }

    private def withSession(sessionId:String)(fn:(SessionService) => server.Route) : server.Route = {
        sessionManager.getSession(sessionId) match {
            case Some(session) => fn(session)
            case None => complete(StatusCodes.NotFound -> s"Session $sessionId not found")
        }
    }
}
