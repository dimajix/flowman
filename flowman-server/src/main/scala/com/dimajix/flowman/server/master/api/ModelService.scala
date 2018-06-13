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


@Api(value = "models", produces = "application/json")
@Path("/namespaces/{namespace}/projects/{project}/models")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "namespace", value = "namespace to operate on", required = true,
        dataType = "string", paramType = "path", defaultValue = "default"),
    new ApiImplicitParam(name = "project", value = "project to operate on", required = true,
        dataType = "string", paramType = "path", defaultValue = "default")
))
class ModelService {
    import Directives._

    def route() : server.Route = {
        pathPrefix("namespaces" / Segment) { ns =>
            pathPrefix("projects" / Segment) { project =>
                pathPrefix("models") {
                    list(ns) ~
                    crud(ns) ~
                    misc(ns)
                }
            }
        }
    }

    @ApiOperation(value = "Returns all available models of a project", nickname = "getModels", httpMethod = "GET")
    private def list(@ApiParam(hidden = true) namespace:String): server.Route = {
        pathEnd {
            get {
                val result = "list_models"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    def crud(@ApiParam(hidden = true) namespace: String) : server.Route = {
        path(Segment) { model =>
            create(model) ~
            describe(model) ~
            destroy(model)
        }
    }

    def misc(@ApiParam(hidden = true) namespace: String) : server.Route = {
        pathPrefix(Segment) { model =>
            instance(model) ~
            records(model)
        }
    }

    def instance(model: String) : server.Route = {
        path("instance") {
            createInstance(model) ~
            destroyInstance(model)
        }
    }


    @Path("/{model}/instance")
    @ApiOperation(value = "Retrieves the schema of a model", nickname = "getSchema", httpMethod = "PUT")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "model", value = "name of model to inspect", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def createInstance(@ApiParam(hidden = true) model:String): server.Route = {
        put {
            val result = "create_instance"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{model}/instance")
    @ApiOperation(value = "Retrieves the schema of a model", nickname = "getSchema", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "model", value = "name of model to inspect", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def destroyInstance(@ApiParam(hidden = true) model:String): server.Route = {
        delete {
            val result = "destroy_instance"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{model}/records")
    @ApiOperation(value = "Retrieves some sample records from a model", nickname = "showRecords", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "model", value = "name of model to inspect", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def records(@ApiParam(hidden = true) model:String): server.Route = {
        path("records") {
            get {
                val result = "get_schema"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    @Path("/{model}")
    @ApiOperation(value = "Creates a new model", nickname = "createModel", httpMethod = "PUT")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "model", value = "name of model to create", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def create(@ApiParam(hidden = true) model:String): server.Route = {
        put {
            val result = "create_model"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{model}")
    @ApiOperation(value = "Describes an existing model", nickname = "describeModel", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "model", value = "model to describe", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def describe(@ApiParam(hidden = true) model:String) : server.Route = {
        get {
            val result = "describe_model"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{model}")
    @ApiOperation(value = "Destroys an existing model", nickname = "destroyModel", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "model", value = "model to delete from project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def destroy(@ApiParam(hidden = true) model:String) : server.Route = {
        delete {
            val result = "delete_model"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }
}
