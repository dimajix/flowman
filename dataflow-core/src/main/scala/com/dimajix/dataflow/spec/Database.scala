package com.dimajix.dataflow.spec

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.dataflow.execution.Context

class Database {
    @JsonProperty(value="driver", required=true) private[spec] var _driver:String = _
    @JsonProperty(value="url", required=true) private[spec] var _url:String = _
    @JsonProperty(value="username", required=true) private[spec] var _username:String = _
    @JsonProperty(value="password", required=true) private[spec] var _password:String = _
    @JsonProperty(value="properties") private var _properties:Map[String,String] = Map()

    def driver(implicit context: Context) : String = context.evaluate(_driver)
    def url(implicit context: Context) : String = context.evaluate(_url)
    def username(implicit context: Context) : String  = context.evaluate(_username)
    def password(implicit context: Context) : String = context.evaluate(_password)
    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)
}
