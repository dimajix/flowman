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

package com.dimajix.flowman.server.master.api

import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path


@Api(value = "projects", produces = "application/json")
@Path("/namespaces/{namespace}/projects")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "namespace", value = "namespace to operate on", required = true,
        dataType = "string", paramType = "path", defaultValue = "default")
))
class ProjectService {
    import Directives._

    def route() : server.Route = {
        pathPrefix("namespaces" / Segment) { ns =>
            pathPrefix("projects") {
                list(ns) ~
                crud(ns)
            }
        }
    }

    @ApiOperation(value = "Returns all available projects", nickname = "getProjects", httpMethod = "GET")
    private def list(@ApiParam(hidden = true) namespace:String): server.Route = {
        pathEnd {
            get {
                val result = "list_projects"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    def crud(@ApiParam(hidden = true) namespace: String) : server.Route = {
        path(Segment) { project =>
            create(project) ~
            describe(project) ~
            destroy(project)
        }
    }

    @Path("/{project}")
    @ApiOperation(value = "Creates a new project", nickname = "createProject", httpMethod = "PUT")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of mapping to create", required = true,
            dataType = "string", paramType = "path", defaultValue = "default")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def create(@ApiParam(hidden = true) project:String): server.Route = {
        put {
            val result = "create_project"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{project}")
    @ApiOperation(value = "Describes an existing project", nickname = "describeProject", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "project to describe", required = true,
            dataType = "string", paramType = "path", defaultValue = "default")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def describe(@ApiParam(hidden = true) project:String) : server.Route = {
        get {
            val result = "describe_project"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{project}")
    @ApiOperation(value = "Destroys an existing project", nickname = "destroyProject", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "project to delete from namespace", required = true,
            dataType = "string", paramType = "path", defaultValue = "default")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def destroy(@ApiParam(hidden = true) project:String) : server.Route = {
        delete {
            val result = "delete_project"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }
}
