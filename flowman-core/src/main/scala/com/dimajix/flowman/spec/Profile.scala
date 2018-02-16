package com.dimajix.flowman.spec

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.util.splitSettings


class Profile {
    @JsonProperty(value="enabled") private var _enabled : Boolean = true
    @JsonProperty(value="environment") private var _environment: Seq[String] = Seq()
    @JsonProperty(value="config") private var _config: Seq[String] = Seq()
    @JsonProperty(value="databases") private var _databases: Map[String,Connection] = Map()

    /**
      * Returns true if the profile is enabled
      *
      * @return
      */
    def enabled : Boolean = _enabled

    /**
      * Returns a map of all configured databases
      *
      * @return
      */
    def databases : Map[String,Connection] = _databases

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
