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

package com.dimajix.flowman.tools.shell

import org.jline.reader.Candidate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class CommandCompleterTest extends AnyFlatSpec with Matchers {
    "The CommandCompleter" should "work" in {
        val completer = new CommandCompleter()
        val candidates = new java.util.LinkedList[Candidate]()

        candidates.clear()
        //completer.complete(null, "map", candidates) should be (3)
        //candidates.asScala should be (Seq("mapping"))

        candidates.clear()
        //completer.complete("map ", 4, candidates) should be (4)
        //candidates.asScala should be (Seq())
    }
}
