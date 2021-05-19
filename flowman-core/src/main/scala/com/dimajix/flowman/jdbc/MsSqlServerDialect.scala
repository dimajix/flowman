/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.util.Locale

import org.apache.spark.sql.jdbc.JdbcType

import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.BooleanType
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.ShortType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.TimestampType


object MsSqlServerDialect extends BaseDialect {
    override def canHandle(url : String): Boolean = url.toLowerCase(Locale.ROOT).startsWith("jdbc:sqlserver")

    override def quoteIdentifier(colName: String): String = {
        s""""$colName""""
    }

    override def getJdbcType(dt: FieldType): Option[JdbcType] = dt match {
        case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
        case StringType => Some(JdbcType("NVARCHAR(MAX)", java.sql.Types.NVARCHAR))
        case BooleanType => Some(JdbcType("BIT", java.sql.Types.BIT))
        case BinaryType => Some(JdbcType("VARBINARY(MAX)", java.sql.Types.VARBINARY))
        case ShortType => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
        case _ => None
    }
}
