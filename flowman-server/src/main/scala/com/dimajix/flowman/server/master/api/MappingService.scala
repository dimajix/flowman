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


@Api(value = "mappings", produces = "application/json")
@Path("/mappings/{namespace}/{project}")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "namespace", value = "namespace to operate on", required = true,
        dataType = "string", paramType = "path", defaultValue = "default"),
    new ApiImplicitParam(name = "project", value = "project to operate on", required = true,
        dataType = "string", paramType = "path", defaultValue = "default")
))
class MappingService {
    import Directives._

    def route() : server.Route = {
        pathPrefix("mappings" / Segment) { ns =>
            pathPrefix(Segment) { project =>
                list(ns) ~
                crud(ns) ~
                misc(ns)
            }
        }
    }

    @ApiOperation(value = "Returns all available mappings of a project", nickname = "getMappings", httpMethod = "GET")
    private def list(@ApiParam(hidden = true) namespace:String): server.Route = {
        pathEnd {
            get {
                val result = "list_mappings"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    def crud(@ApiParam(hidden = true) namespace: String) : server.Route = {
        path(Segment) { mapping =>
            create(mapping) ~
            describe(mapping) ~
            destroy(mapping)
        }
    }

    def misc(@ApiParam(hidden = true) namespace: String) : server.Route = {
        pathPrefix(Segment) { mapping =>
            status(mapping) ~
            schema(mapping) ~
            plan(mapping) ~
            records(mapping)
        }
    }


    @Path("/{mapping}/status")
    @ApiOperation(value = "Validates a mapping", nickname = "validateMapping", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "name of mapping to validate", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def status(@ApiParam(hidden = true) mapping:String): server.Route = {
        path("status") {
            get {
                val result = "get_status"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    @Path("/{mapping}/plan")
    @ApiOperation(value = "Explains the execution plan of a mapping", nickname = "explainMapping", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "name of mapping to explain", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def plan(@ApiParam(hidden = true) mapping:String): server.Route = {
        path("plan") {
            get {
                val result = "get_plan"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    @Path("/{mapping}/schema")
    @ApiOperation(value = "Retrieves the schema of a mapping", nickname = "getSchema", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "name of mapping to inspect", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def schema(@ApiParam(hidden = true) mapping:String): server.Route = {
        path("schema") {
            get {
                val result = "get_schema"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    @Path("/{mapping}/records")
    @ApiOperation(value = "Retrieves some sample records from a mapping", nickname = "showRecords", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "name of mapping to inspect", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def records(@ApiParam(hidden = true) mapping:String): server.Route = {
        path("records") {
            get {
                val result = "get_schema"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    @Path("/{mapping}")
    @ApiOperation(value = "Creates a new mapping", nickname = "createMapping", httpMethod = "PUT")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "name of mapping to create", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def create(@ApiParam(hidden = true) mapping:String): server.Route = {
        put {
            val result = "create_mapping"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{mapping}")
    @ApiOperation(value = "Describes an existing mapping", nickname = "describeMapping", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "mapping to describe", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def describe(@ApiParam(hidden = true) mapping:String) : server.Route = {
        get {
            val result = "describe_mapping"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{mapping}")
    @ApiOperation(value = "Destroys an existing mapping", nickname = "destroyMapping", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "mapping to delete from project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def destroy(@ApiParam(hidden = true) mapping:String) : server.Route = {
        delete {
            val result = "delete_mapping"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }
}
