package com.dimajix.flowman.spec

import com.dimajix.flowman.spec.runner.Runner
import com.dimajix.flowman.storage.Store


object Namespace {

}


class Namespace {
    private var _store: Store = _
    private var _name: String = "default"
    private var _environment: Seq[(String,String)] = Seq()
    private var _config: Seq[(String,String)] = Seq()
    private var _profiles: Map[String,Profile] = Map()
    private var _runner : Runner = _

    def name : String = _name

    def config : Seq[(String,String)] = _config
    def environment : Seq[(String,String)] = _environment

    def profiles : Map[String,Profile] = _profiles
    def projects : Seq[String] = ???
    def runner : Runner = _runner
}
