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

sealed abstract class TestStatus extends Product with Serializable

object TestStatus {
    final case object FAILED extends TestStatus
    final case object SUCCESS extends TestStatus
    final case object ERROR extends TestStatus
}


final case class TestResultReference(
    parent:Option[Reference]
) extends Reference


final case class TestResult(
    parent:Some[Reference],
    status:TestStatus,
    description:Option[String],
    details:Option[Fragment]
) extends Fragment {
    override def reference: TestResultReference = TestResultReference(parent)
    override def fragments: Seq[Fragment] = details.toSeq

    override def reparent(parent:Reference) : TestResult = {
        val ref = TestResultReference(Some(parent))
        copy(
            parent = Some(parent),
            details = details.map(_.reparent(ref))
        )
    }
}
