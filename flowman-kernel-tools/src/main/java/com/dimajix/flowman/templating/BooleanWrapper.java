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


public class BooleanWrapper {
    public boolean parse(boolean value) {
        return value;
    }
    public boolean parse(Boolean value) {
        return value;
    }
    public boolean parse(String value) {
        return Boolean.parseBoolean(value);
    }
    public boolean valueOf(String value) {
        return Boolean.parseBoolean(value);
    }
}
