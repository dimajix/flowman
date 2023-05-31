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

package com.dimajix.common.logging;

import java.util.List;

import lombok.Value;
import org.slf4j.Marker;
import org.slf4j.event.KeyValuePair;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;


@Value
public class LogEvent implements LoggingEvent {
    Level level;
    String loggerName;
    String message;
    long timeStamp;
    Throwable throwable;

    @Override
    public String getThreadName() { return null; }

    @Override
    public Object[] getArgumentArray() { return null; }

    @Override
    public List<Object> getArguments() { return null; }

    @Override
    public List<Marker> getMarkers() { return null; }

    @Override
    public List<KeyValuePair> getKeyValuePairs() { return null; }
}
