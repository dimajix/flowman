package com.dimajix.dataflow.spec

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.util.splitSettings


class Profile {
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonProperty(value="databases") private var _databases: Map[String,Database] = Map()

    def databases : Map[String,Database] = _databases

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
}
