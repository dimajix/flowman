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


@Api(value = "/namespaces", produces = "application/json")
@Path("/namespaces")
class NamespaceService {
    import Directives._

    def route() : server.Route = {
        pathPrefix("namespaces") {
            list() ~
            crud()
        }
    }

    @ApiOperation(value = "Returns all available namespaces", nickname = "getNamespaces", httpMethod = "GET")
    def list() : server.Route = {
        pathEnd {
            get {
                val result = "list_namespace"
                complete(HttpEntity(ContentTypes.`application/json`, result))
            }
        }
    }

    def crud() : server.Route = {
        path(Segment) { namespace =>
            create(namespace) ~
            describe(namespace) ~
            destroy(namespace)
        }
    }

    @Path("/{namespace}")
    @ApiOperation(value = "Creates a new namespaces", nickname = "createNamespace", httpMethod = "PUT")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "namespace", value = "namespace to operate on", required = true,
            dataType = "string", paramType = "path", defaultValue = "default")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def create(@ApiParam(hidden = true) namespace:String): server.Route = {
        put {
            val result = "create_namespace"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{namespace}")
    @ApiOperation(value = "Describes an existing namespace", nickname = "describeNamespace", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "namespace", value = "namespace to operate on", required = true,
            dataType = "string", paramType = "path", defaultValue = "default")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def describe(@ApiParam(hidden = true) namespace:String) : server.Route = {
        get {
            val result = "describe_namespace"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }

    @Path("/{namespace}")
    @ApiOperation(value = "Destroys an existing namespace", nickname = "destroyNamespace", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "namespace", value = "namespace to operate on", required = true,
            dataType = "string", paramType = "path", defaultValue = "default")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "", response = classOf[String]),
        new ApiResponse(code = 500, message = "Internal server error")
    ))
    def destroy(@ApiParam(hidden = true) namespace:String) : server.Route = {
        delete {
            val result = "delete_namespace"
            complete(HttpEntity(ContentTypes.`application/json`, result))
        }
    }
}
