package com.dimajix.dataflow.spec

import java.io.File

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.flow.Mapping
import com.dimajix.dataflow.spec.model.Relation
import com.dimajix.dataflow.spec.output.Output
import com.dimajix.dataflow.util.splitSettings


object Module {
    private val logger = LoggerFactory.getLogger(classOf[Project])

    private def mapper = {
        val relationTypes = Relation.getProviders.map(p => new NamedType(p.getClass, p.getName))
        val mappingTypes = Mapping.getProviders.map(p => new NamedType(p.getClass, p.getName))
        val mapper = new ObjectMapper(new YAMLFactory())
        mapper.registerModule(DefaultScalaModule)
        mapper.registerSubtypes(relationTypes:_*)
        mapper.registerSubtypes(mappingTypes:_*)
        mapper
    }
    private def loadFile(file:File) : Module = {
        logger.info(s"Reading module file ${file.toString}")
        mapper.readValue(file, classOf[Module])
    }

    /**
      * Loads a single file or a whole directory (non recursibely)
      *
      * @param file
      * @return
      */
    def load(file:File) : Module = {
        if (file.isDirectory) {
            logger.info(s"Reading all module files in directory ${file.toString}")
            file.listFiles()
                .filter(_.isFile)
                .map(f => loadFile(f))
                .reduce((l,r) => l.merge(r))
        }
        else {
            loadFile(file)
        }
    }
    /**
      * Loads a single file or a whole directory (non recursibely)
      *
      * @param filename
      * @return
      */
    def load(filename:String) : Module = {
        load(new File(filename))
    }

    def parse(text:String) : Module = {
        mapper.readValue(text, classOf[Module])
    }
}



class Module {
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonProperty(value="profiles") private var _profiles: Map[String,Profile] = Map()
    @JsonProperty(value="databases") private var _databases: Map[String,Database] = Map()
    @JsonProperty(value="models") private var _models: Map[String,Relation] = Map()
    @JsonProperty(value="dataflow") private var _dataflow: Map[String,Mapping] = Map()
    @JsonProperty(value="outputs") private var _outputs: Map[String,Output] = Map()

    def profiles : Map[String,Profile] = _profiles
    def models : Map[String,Relation] = _models
    def databases : Map[String,Database] = _databases
    def transforms : Map[String,Mapping] = _dataflow
    def outputs : Map[String,Output] = _outputs

    /**
      * Returns all configuration variables as a key-value sequence
      *
      * @return
      */
    def config : Seq[(String,String)] = splitSettings(_config)

    /**
      * Returns the environment as a key-value-sequence
      *
      * @return
      */
    def environment : Seq[(String,String)] = splitSettings(_environment)

    /**
      * Creates a new dataflow by merging this one with another one.
      *
      * @param other
      * @return
      */
    def merge(other:Module) : Module = {
        val result = new Module
        result._environment = _environment ++ other._environment
        result._config = _config ++ other._config
        result._databases = _databases ++ other._databases
        result._models = _models ++ other._models
        result._dataflow = _dataflow ++ other._dataflow
        result._outputs = _outputs ++ other._outputs
        result._profiles = _profiles ++ other._profiles
        result
    }
}
