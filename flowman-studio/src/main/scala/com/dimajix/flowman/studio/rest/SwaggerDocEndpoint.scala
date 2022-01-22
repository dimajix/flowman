package com.dimajix.flowman.studio.rest

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.models.auth.BasicAuthDefinition

import com.dimajix.flowman.studio.rest.session.JobEndpoint
import com.dimajix.flowman.studio.rest.session.SessionEndpoint
import com.dimajix.flowman.studio.rest.workspace.ParcelEndpoint
import com.dimajix.flowman.studio.rest.workspace.ProjectEndpoint
import com.dimajix.flowman.studio.rest.workspace.WorkspaceEndpoint


object SwaggerDocEndpoint extends SwaggerHttpService  {
    override def apiClasses = Set(
        classOf[ShutdownEndpoint],
        classOf[WorkspaceEndpoint],
        classOf[ParcelEndpoint],
        classOf[PingEndpoint],
        classOf[ProjectEndpoint],
        classOf[NamespaceEndpoint],
        classOf[SessionEndpoint],
        classOf[JobEndpoint]
    )
    override def host = ""
    override def basePath: String = "/api/"
    override def apiDocsPath: String = "swagger"
    override def info = Info(version = "1.0")
    // override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
    // override def securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition())
    override val unwantedDefinitions = Seq()
}
