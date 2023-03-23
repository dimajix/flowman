/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.documentation

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.documentation.Documenter
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.Spec


final class DocumenterSpec extends Spec[Documenter] {
    @JsonProperty(value="collectors") private var collectors: Seq[CollectorSpec] = Seq()
    @JsonProperty(value="generators") private var generators: Seq[GeneratorSpec] = Seq()

    def instantiate(context:Context, properties:Option[Documenter.Properties]): Documenter = {
        Documenter(
            collectors.map(_.instantiate(context)),
            generators.map(_.instantiate(context))
        )
    }
}
