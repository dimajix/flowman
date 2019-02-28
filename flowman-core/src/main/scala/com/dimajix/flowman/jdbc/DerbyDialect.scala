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

package com.dimajix.flowman.jdbc

import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.ByteType
import com.dimajix.flowman.types.DecimalType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType


object DerbyDialect extends BaseDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:derby")

    override def getJdbcType(dt: FieldType): Option[JdbcType] = dt match {
        case StringType => Option(JdbcType("CLOB", java.sql.Types.CLOB))
        case ByteType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
        case ShortType => Option(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
        case BooleanType => Option(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
        // 31 is the maximum precision and 5 is the default scale for a Derby DECIMAL
        case t: DecimalType if t.precision > 31 =>
            Option(JdbcType("DECIMAL(31,5)", java.sql.Types.DECIMAL))
        case _ => super.getJdbcType(dt)
    }

}
