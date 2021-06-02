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

import java.net.URL

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
import javax.ws.rs.POST
import javax.ws.rs.Path

import com.dimajix.flowman.studio.model.KernelRegistrationRequest
import com.dimajix.flowman.studio.service.KernelManager
import com.dimajix.flowman.studio.service.KernelService


@Api(value = "registry", produces = "application/json", consumes = "application/json")
@Path("/registry")
class RegistryEndpoint(kernelManager:KernelManager) {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes : server.Route = pathPrefix("registry") {(
        pathEndOrSingleSlash {(
            registerKernel()
        )}
        ~
        pathPrefix(Segment) { kernel => (
            pathEndOrSingleSlash {(
                removeKernel(kernel)
            )}
        )}
    )}

    @POST
    @ApiOperation(value = "Register kernel", nickname = "registerKernel", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel registration request", value = "Kernel parameters", required = true,
            dataTypeClass = classOf[KernelRegistrationRequest], paramType = "body")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully registered kernel"),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    private def registerKernel() : server.Route = {
        post {
            entity(as[KernelRegistrationRequest]) { request =>
                kernelManager.registerKernel(request.id, new URL(request.url))
                complete("success")
            }
        }
    }

    @DELETE
    @Path("/{kernel}")
    @ApiOperation(value = "Unregister kernel", nickname = "unregisterKernel", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully removed kernel"),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    private def removeKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        delete {
            kernelManager.getKernel(kernel) match {
                case None =>
                    complete(HttpResponse(status = StatusCodes.NotFound))
                case Some(kernel) =>
                    kernelManager.unregisterKernel(kernel)
                    complete("success")
            }
        }
    }
}
