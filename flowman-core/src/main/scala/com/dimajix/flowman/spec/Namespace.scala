/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
