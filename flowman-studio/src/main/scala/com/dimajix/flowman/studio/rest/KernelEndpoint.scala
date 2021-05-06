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
import akka.http.scaladsl.model.StatusCodes.Found
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
import javax.ws.rs.PUT
import javax.ws.rs.Path

import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model.Kernel
import com.dimajix.flowman.studio.model.KernelList
import com.dimajix.flowman.studio.service.KernelManager
import com.dimajix.flowman.studio.service.KernelService


@Api(value = "kernel", produces = "application/json", consumes = "application/json")
@Path("/kernel")
class KernelEndpoint(kernelManager:KernelManager) {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes : server.Route = pathPrefix("kernel") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(Found) {(
                createKernel()
                ~
                listKernel()
            )}
        }
        ~
        pathPrefix(Segment) { kernel => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(Found) {(
                    getKernel(kernel)
                    ~
                    stopKernel(kernel)
                )}
            }
            ~
            invokeKernel(kernel)
        )}
    )}

    @POST
    @ApiOperation(value = "create", nickname = "createKernel", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Create new kernel", response = classOf[Kernel])
    ))
    private def createKernel() : server.Route = {
        post {
            ???
        }
    }

    @GET
    @ApiOperation(value = "list", nickname = "listKernels", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of kernels", response = classOf[KernelList])
    ))
    private def listKernel() : server.Route = {
        get {
            val result = KernelList(kernelManager.list().map(Converter.of))
            complete(result)
        }
    }

    @GET
    @Path("/{kernel}")
    @ApiOperation(value = "Get kernel", nickname = "getKernel", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Retrieve information about a  specific kernel", response = classOf[Kernel]),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    private def getKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        get {
            withKernel(kernel) { kernel =>
                val result = Converter.of(kernel)
                complete(result)
            }
        }
    }

    @DELETE
    @Path("/{kernel}")
    @ApiOperation(value = "Shutdown kernel", nickname = "stopKernel", httpMethod = "DELETE")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully stopped kernel", response = classOf[String]),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    private def stopKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        delete {
            withKernel(kernel) { kernel =>
                kernel.shutdown()
                complete("success")
            }
        }
    }

    @GET
    @PUT
    @POST
    @DELETE
    @Path("/{kernel}/")
    @ApiOperation(value = "Invoke kernel", nickname = "invokeKernel", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "kernel", value = "Kernel ID", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Successfully invoked kernel"),
        new ApiResponse(code = 404, message = "Kernel not found")
    ))
    private def invokeKernel(@ApiParam(hidden = true) kernel:String) : server.Route = {
        withKernel(kernel) { kernel =>
            extractUnmatchedPath { path =>
                extractRequest { request =>
                    val uri = request.uri.withPath(path)
                    val newRequest = request.withUri(uri)
                    complete(kernel.invoke(newRequest))
                }
            }
        }
    }

    private def withKernel(kernel:String)(fn:KernelService => server.Route) : server.Route = {
        kernelManager.getKernel(kernel) match {
            case Some(svc) => fn(svc)
            case None => complete(HttpResponse(status = StatusCodes.NotFound))
        }
    }
}
