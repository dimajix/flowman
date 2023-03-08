/*
 * Copyright (C) 2018 The Flowman Authors
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


sealed abstract class Phase extends Product with Serializable {
    def lower : String = toString.toLowerCase(Locale.ROOT)
    def upper : String = toString.toUpperCase(Locale.ROOT)
}

object Phase {
    case object VALIDATE extends Phase
    case object CREATE extends Phase
    case object BUILD extends Phase
    case object VERIFY extends Phase
    case object TRUNCATE extends Phase
    case object DESTROY extends Phase

    def ofString(status:String) : Phase = {
        status.toLowerCase(Locale.ROOT) match {
            case "validate" => VALIDATE
            case "create" => CREATE
            case "build" => BUILD
            case "verify" => VERIFY
            case "truncate" => TRUNCATE
            case "destroy" => DESTROY
            case _ => throw new IllegalArgumentException(s"No phase defined for '$status'")
        }
    }
}


/**
  * This object contains predefined lifecycles, where each lifecycle contains a specific sequence of phases to be
  * executed.
  */
object Lifecycle {
    val BUILD:Seq[Phase] = Seq(
        Phase.VALIDATE,
        Phase.CREATE,
        Phase.BUILD,
        Phase.VERIFY
    )
    val CLEAN:Seq[Phase] = Seq(
        Phase.TRUNCATE
    )
    val DESTROY:Seq[Phase] = Seq(
        Phase.DESTROY
    )

    val ALL:Seq[Phase] = BUILD ++ CLEAN ++ DESTROY
    val DEFAULT:Seq[Phase] = BUILD

    private val all = Seq(DEFAULT, CLEAN, DESTROY)

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
