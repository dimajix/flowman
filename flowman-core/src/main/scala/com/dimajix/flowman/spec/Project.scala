package com.dimajix.flowman.spec

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.runner.Runner

class ProjectReader {
    private val logger = LoggerFactory.getLogger(classOf[ProjectReader])

    private def mapper = {
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper
    }

    /**
      * Loads a project file and all related module files
      *
      * @param file
      * @return
      */
    def file(file:File) : Project = {
        logger.info(s"Reading project file ${file.toString}")
        val project = mapper.readValue(file, classOf[Project])
        loadModules(project, file.getParentFile)
        project
    }

    /**
      * Loads a project file
      *
      * @param filename
      * @return
      */
    def file(filename:String) : Project = {
        file(new File(filename))
    }

    def string(text:String) : Project = {
        mapper.readValue(text, classOf[Project])
    }

    private def loadModules(project: Project, directory:File) : Unit = {
        val module = project.modules
            .map(f => Module.read.file(new File(directory, f)))
            .reduce((l,r) => l.merge(r))

        project._environment = module.environment
        project._config = module.config
        project._profiles = module.profiles
        project._connections = module.connections
        project._relations = module.relations
        project._mappings = module.mappings
        project._outputs = module.outputs
    }
}


object Project {
    def read = new ProjectReader
}


class Project {
    @JsonProperty(value="name") private[spec] var _name: String = _
    @JsonProperty(value="version") private[spec] var _version: String = _
    @JsonProperty(value="modules") private[spec] var _modules: Seq[String] = Seq()

    private[spec] var _environment: Seq[(String,String)] = Seq()
    private[spec] var _config: Seq[(String,String)] = Seq()
    private[spec] var _profiles: Map[String,Profile] = Map()
    private[spec] var _connections: Map[String,Connection] = Map()
    private[spec] var _relations: Map[String,Relation] = Map()
    private[spec] var _mappings: Map[String,Mapping] = Map()
    private[spec] var _outputs: Map[String,Output] = Map()
    private[spec] var _jobs: Map[String,Job] = Map()
    private[spec] var _runner: Runner = _

    def name : String = _name
    def version : String = _version
    def modules : Seq[String] = _modules

    def config : Seq[(String,String)] = _config
    def environment : Seq[(String,String)] = _environment
    def runner : Runner = _runner

    def profiles : Map[String,Profile] = _profiles
    def relations : Map[String,Relation] = _relations
    def connections : Map[String,Connection] = _connections
    def mappings : Map[String,Mapping] = _mappings
    def outputs : Map[String,Output] = _outputs
    def jobs : Map[String,Job] = _jobs
}

