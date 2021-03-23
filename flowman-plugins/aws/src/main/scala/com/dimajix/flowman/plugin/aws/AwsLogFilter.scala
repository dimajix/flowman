/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.plugin.aws

import com.dimajix.flowman.plugin.aws.AwsLogFilter.redactedKeys
import com.dimajix.flowman.spi.LogFilter

object AwsLogFilter {
    val redactedKeys = Set(
        "spark.hadoop.fs.s3a.proxy.password",
        "spark.hadoop.fs.s3a.access.key",
        "spark.hadoop.fs.s3a.secret.key"
    )
}

class AwsLogFilter extends LogFilter {
    override def filterConfig(key: String, value: String): Option[(String, String)] = {
        if (redactedKeys contains key) {
            Some((key, "***redacted***"))
        }
        else {
            Some((key, value))
        }
    }
}
