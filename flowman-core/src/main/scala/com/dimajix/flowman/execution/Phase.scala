/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import java.util.Locale


sealed abstract class Phase  {
    val value:String

    override def toString: String = value
}

object Phase {
    case object CREATE extends Phase {
        override val value = "create"
    }
    case object MIGRATE extends Phase {
        override val value = "migrate"
    }
    case object BUILD extends Phase {
        override val value = "build"
    }
    case object VERIFY extends Phase {
        override val value = "verify"
    }
    case object TRUNCATE extends Phase {
        override val value = "truncate"
    }
    case object DESTROY extends Phase {
        override val value = "destroy"
    }

    def ofString(status:String) : Phase = {
        status.toLowerCase(Locale.ROOT) match {
            case CREATE.value => CREATE
            case MIGRATE.value => MIGRATE
            case BUILD.value => BUILD
            case VERIFY.value => VERIFY
            case TRUNCATE.value => TRUNCATE
            case DESTROY.value => DESTROY
            case _ => throw new IllegalArgumentException(s"No phase defined for '$status'")
        }
    }
}


/**
  * This object contains predefined lifecycles, where each lifecycle contains a specific sequence of phases to be
  * executed.
  */
object Lifecycle {
    val DEFAULT = Seq(
        Phase.CREATE,
        Phase.MIGRATE,
        Phase.BUILD,
        Phase.VERIFY
    )
    val CLEAN = Seq(
        Phase.TRUNCATE,
        Phase.DESTROY
    )

    private val all = Seq(DEFAULT, CLEAN)

    /**
      * Creates an appropriate lifecycle from the beginning up to the specified phase
      * @param phase
      * @return
      */
    def ofPhase(phase:Phase) : Seq[Phase] = {
        all.map { lifecycle =>
                val idx = lifecycle.indexOf(phase)
                lifecycle.take(idx + 1)
            }
            .filter(_.nonEmpty)
            .head
    }
}
