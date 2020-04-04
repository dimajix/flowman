package com.dimajix.flowman.server.model

case class Project(
    name:String,
    version:Option[String],
    description: Option[String],
    environment: Map[String,String],
    config: Map[String,String],
    profiles: Seq[String],
    connections: Seq[String],
    basedir: String
) {
}
