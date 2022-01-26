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

package com.dimajix.flowman.studio.rest.workspace

import java.nio.file.Files

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.scaladsl.FileIO
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
import org.apache.hadoop.conf.Configuration

import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.spec.storage.LocalParcel
import com.dimajix.flowman.storage.Parcel
import com.dimajix.flowman.storage.Workspace
import com.dimajix.flowman.studio.model
import com.dimajix.flowman.studio.model.Converter


@Api(value = "workspace", produces = "application/json", consumes = "application/json")
@Path("/workspace/{workspace}/parcel")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "workspace", value = "name of workspace", required = true,
        dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class ParcelEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(workspace:Workspace) : Route = pathPrefix("parcel") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listParcels(workspace)
            }
        }
        ~
        pathPrefix(Segment) { parcel =>
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {(
                    createParcel(workspace, parcel)
                    ~
                    withParcel(workspace, parcel) { parcel => (
                        getParcel(workspace, parcel)
                        ~
                        updateParcel(workspace, parcel)
                        ~
                        deleteParcel(workspace, parcel)
                    )}
                )}
            }
        })
    }

    @GET
    @ApiOperation(value = "Return list of all parcels within a workspace", nickname = "listParcels", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all parcels", response = classOf[model.ParcelList])
    ))
    def listParcels(@ApiParam(hidden = true) workspace:Workspace) : server.Route = {
        get {
            val result = model.ParcelList(workspace.parcels.map(p => Converter.of(p)))
            complete(result)
        }
    }

    @GET
    @Path("/{parcel}")
    @ApiOperation(value = "Returns information on one parcel", nickname = "getParcel", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all workspaces", response = classOf[model.Parcel])
    ))
    def getParcel(@ApiParam(hidden = true) workspace:Workspace, @ApiParam(hidden = true) parcel:Parcel) : server.Route = {
        get {
            complete(Converter.of(parcel))
        }
    }

    @POST
    @Path("/{parcel}")
    @ApiOperation(value = "Create a new parcel", nickname = "createParcel", httpMethod = "POST")
    @ApiResponses(Array(
        new ApiResponse(code = 201, message = "Parcel successfully created", response = classOf[model.Parcel])
    ))
    def createParcel(@ApiParam(hidden = true) workspace:Workspace, @ApiParam(hidden = true) parcel:String) : server.Route = {
        post {
            if (workspace.parcels.exists(_.name == parcel)) {
                complete(StatusCodes.BadRequest -> s"A parcel '$parcel' already exists in workspace '${workspace.name}'")
            }
            else {
                val instance = LocalParcel(parcel, workspace.root / parcel)
                workspace.addParcel(instance)
                complete(StatusCodes.Created -> Converter.of(instance))
            }
        }
    }

    @PUT
    @Path("/{parcel}")
    @ApiOperation(value = "Upload new contents to an existing parcel", nickname = "updateParcel", httpMethod = "PUT")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Parcel successfully updated")
    ))
    def updateParcel(@ApiParam(hidden = true) workspace:Workspace, @ApiParam(hidden = true) parcel:Parcel) : server.Route = {
        put {
            extractRequestEntity { entity =>
                extractRequestContext { ctx =>
                    implicit val materializer: Materializer = ctx.materializer
                    val temp = Files.createTempFile("flowman-parcel", ".tgz")
                    val upload = entity.dataBytes.runWith(FileIO.toPath(temp))
                    onComplete(upload) { _ =>
                        val fs = FileSystem(new Configuration())
                        val tempFile = fs.local(temp.toFile)
                        parcel.replace(tempFile)
                        complete(StatusCodes.OK)
                    }
                }
            }
        }
    }

    @DELETE
    @Path("/{parcel}")
    @ApiOperation(value = "Deletes a specific parcel", nickname = "deleteParcel", httpMethod = "DELETE")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Deletes a specific parcel")
    ))
    def deleteParcel(@ApiParam(hidden = true) workspace:Workspace, @ApiParam(hidden = true) parcel:Parcel) : server.Route = {
        delete {
            workspace.removeParcel(parcel.name)
            complete(HttpResponse(status = StatusCodes.OK))
        }
    }

    private def withParcel(workspace:Workspace, name:String)(fn:Parcel => server.Route) : server.Route = {
        workspace.parcels.find(_.name == name) match {
            case Some(parcel) => fn(parcel)
            case None => complete(StatusCodes.NotFound -> s"Parcel '$name' not found in workspace '${workspace.name}'")
        }
    }
}
