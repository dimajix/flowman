/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.templating;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import lombok.val;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils;

import com.dimajix.flowman.types.FieldValue;


public class StringWrapper {
    public String concat(String c1, String c2) {
        return c1 + c2;
    }
    public String concat(String c1, String c2, String c3) {
        return c1 + c2 + c3;
    }
    public String concat(String c1, String c2, String c3, String c4) {
        return c1 + c2 + c3 + c4;
    }
    public String concat(String c1, String c2, String c3, String c4, String c5) {
        return c1 + c2 + c3 + c4 + c5;
    }
    public String urlEncode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, "UTF-8");
    }
    public String escapePathName(String value) {
        return ExternalCatalogUtils.escapePathName(value); //.replace(" ", "%20");
    }
    public String partitionEncode(Object value) {
        if (value.equals("*")) {
            return "*";
        }
        else {
            val str = FieldValue.asString(value);
            return ExternalCatalogUtils.escapePathName(str);
        }
    }
}
