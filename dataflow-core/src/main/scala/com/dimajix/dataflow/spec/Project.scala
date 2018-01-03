package com.dimajix.dataflow.spec

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.flow.Mapping
import com.dimajix.dataflow.spec.model.Relation


object Project {
    private val logger = LoggerFactory.getLogger(classOf[Project])

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
    def load(file:File) : Project = {
        logger.info(s"Reading project file ${file.toString}")
        val project = mapper.readValue(file, classOf[Project])
        val module = project._modules
            .map(f => Module.load(new File(file.getParentFile, f)))
            .reduce((l,r) => l.merge(r))

        project._environment = module.environment
        project._config = module.config
        project._profiles = module.profiles
        project._databases = module.databases
        project._models = module.models
        project._dataflow = module.transforms
        project
    }

    /**
      * Loads a project file
      *
      * @param filename
      * @return
      */
    def load(filename:String) : Project = {
        load(new File(filename))
    }

    def parse(text:String) : Project = {
        mapper.readValue(text, classOf[Project])
    }
}


class Project {
    @JsonProperty(value="name") private var _name: String = _
    @JsonProperty(value="version") private var _version: String = _
    @JsonProperty(value="modules") private var _modules: Seq[String] = Seq()

    private var _environment: Seq[(String,String)] = Seq()
    private var _config: Seq[(String,String)] = Seq()
    private var _profiles: Map[String,Profile] = Map()
    private var _databases: Map[String,Database] = Map()
    private var _models: Map[String,Relation] = Map()
    private var _dataflow: Map[String,Mapping] = Map()

    def name : String = _name
    def version : String = _version
    def modules : Seq[String] = _modules

    def profiles : Map[String,Profile] = _profiles
    def models : Map[String,Relation] = _models
    def databases : Map[String,Database] = _databases
    def transforms : Map[String,Mapping] = _dataflow
    def config : Seq[(String,String)] = _config
    def environment : Seq[(String,String)] = _environment
}

