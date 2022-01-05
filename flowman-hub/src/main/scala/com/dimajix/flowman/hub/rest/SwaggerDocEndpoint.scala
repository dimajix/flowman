package com.dimajix.flowman.hub.rest

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info


object SwaggerDocEndpoint extends SwaggerHttpService  {
    override def apiClasses = Set(
        classOf[KernelEndpoint],
        classOf[PingEndpoint],
        classOf[RegistryEndpoint],
        classOf[LauncherEndpoint]
    )
    override def host = ""
    override def basePath: String = "/api/"
    override def apiDocsPath: String = "swagger"
    override def info = Info(version = "1.0")
    // override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
    // override def securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition())
    override val unwantedDefinitions = Seq()
}
