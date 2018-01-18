package com.dimajix.dataflow.spec

import com.dimajix.dataflow.storage.Store


object Namespace {

}


class Namespace {
    private val _store: Store = _
    private val _name: String = _
    private var _environment: Seq[(String,String)] = Seq()
    private var _config: Seq[(String,String)] = Seq()
    private var _profiles: Map[String,Profile] = Map()

    def name : String = _name

    def config : Seq[(String,String)] = _config
    def environment : Seq[(String,String)] = _environment

    def profiles : Map[String,Profile] = _profiles
    def projects : Seq[String] = ???
}
