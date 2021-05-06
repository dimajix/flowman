/*
 * Copyright 2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model.Kernel
import com.dimajix.flowman.studio.model.LauncherList
import com.dimajix.flowman.studio.service.LauncherManager


@Api(value = "launcher", produces = "application/json", consumes = "application/json")
@Path("/launcher")
class LauncherEndpoint(launcherManager: LauncherManager) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes : server.Route = pathPrefix("kernel") {(
        pathEndOrSingleSlash {
                listLaunchers()
            }
            ~
            pathPrefix(Segment) { kernel =>
                pathEndOrSingleSlash {
                    getLauncher(kernel)
                }
            }
        )
    }

    @GET
    @ApiOperation(value = "list", nickname = "listLaunchers", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of launchers", response = classOf[LauncherList])
    ))
    def listLaunchers() : server.Route = {
        get {
            val result = LauncherList(launcherManager.list().map(Converter.of))
            complete(result)
        }
    }

    @GET
    @Path("/{launcher}")
    @ApiOperation(value = "Get launcher", nickname = "getLauncher", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Retrieve information about a  specific kernel", response = classOf[Kernel]),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    private def getLauncher(@ApiParam(hidden = true) launcher:String) : server.Route = {
        get {
            launcherManager.getLauncher(launcher) match {
                case Some(svc) => complete(Converter.of(svc))
                case None => complete(HttpResponse(status = StatusCodes.NotFound))
            }
        }
    }
}
