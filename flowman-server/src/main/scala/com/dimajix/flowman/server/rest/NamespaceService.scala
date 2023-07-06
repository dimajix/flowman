package com.dimajix.flowman.server.rest

import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.inject.Inject
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

import com.dimajix.flowman.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.Namespace


@Api(value = "namespace", produces = "application/json", consumes = "application/json")
@Path("/namespace")
class NamespaceService @Inject()(ns:model.Namespace) {
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    @ApiOperation(value = "Return information on the current namespace", nickname = "getNamespace", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about namespace", response = classOf[Namespace])
    ))
    def info() : Namespace = {
        Converter.ofSpec(ns)
    }
}
