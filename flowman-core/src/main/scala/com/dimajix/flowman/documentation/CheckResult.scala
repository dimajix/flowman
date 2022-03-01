/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation


sealed abstract class CheckStatus extends Product with Serializable {
    def success : Boolean
    def failure : Boolean
    def run : Boolean
}

object CheckStatus {
    final case object FAILED extends CheckStatus {
        def success : Boolean = false
        def failure : Boolean = true
        def run : Boolean = true
    }
    final case object SUCCESS extends CheckStatus {
        def success : Boolean = true
        def failure : Boolean = false
        def run : Boolean = true
    }
    final case object ERROR extends CheckStatus {
        def success : Boolean = false
        def failure : Boolean = true
        def run : Boolean = true
    }
    final case object NOT_RUN extends CheckStatus {
        def success : Boolean = false
        def failure : Boolean = false
        def run : Boolean = false
    }
}


final case class CheckResultReference(
    parent:Option[Reference]
) extends Reference {
    override def toString: String = {
        parent match {
            case Some(ref) => ref.toString + "/result"
            case None => ""
        }
    }
    override def kind : String = "check_result"
}


final case class CheckResult(
    parent:Some[Reference],
    status:CheckStatus,
    description:Option[String] = None,
    details:Option[Fragment] = None
) extends Fragment {
    override def reference: CheckResultReference = CheckResultReference(parent)
    override def fragments: Seq[Fragment] = details.toSeq

    override def reparent(parent:Reference) : CheckResult = {
        val ref = CheckResultReference(Some(parent))
        copy(
            parent = Some(parent),
            details = details.map(_.reparent(ref))
        )
    }

    def success : Boolean = status.success
    def failure : Boolean = status.failure
    def run : Boolean = status.run
}
