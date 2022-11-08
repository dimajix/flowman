/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.dimajix.flowman.fs

import java.util.regex.Pattern
import java.util.regex.PatternSyntaxException

import org.apache.hadoop.fs.Path


/**
  * A class for POSIX glob pattern with brace expansions.
  */
object GlobPattern {
    /**
      * Compile glob pattern string
      *
      * @param globPattern the glob pattern
      * @return the pattern object
      */
    def compile(globPattern: String): Pattern = new GlobPattern(globPattern).compiled

    private def error(message: String, pattern: String, pos: Int): Unit = {
        throw new PatternSyntaxException(message, pattern, pos)
    }
}


case class GlobPattern(globPattern: String) {
    private var _hasWildcard = false
    private val _compiled = {
        val BACKSLASH = '\\'
        val regex = new StringBuilder
        var setOpen = 0
        var curlyOpen = 0
        var isBackslash = false
        globPattern.foreach {
            case c if isBackslash =>
                regex.append(c)
                isBackslash = false
            case BACKSLASH =>
                regex.append(BACKSLASH)
                isBackslash = true
            case c@('.' | '$' | '(' | ')' | '|' | '+') =>
                // escape regex special chars that are not glob special chars
                regex.append(BACKSLASH)
                regex.append(c)
            case '*' =>
                regex.append("[^/]+")
                _hasWildcard = true
            case '?' =>
                regex.append("[^/\\*]")
                _hasWildcard = true
            case '{' => // start of a group
                regex.append("(?:") // non-capturing
                curlyOpen += 1
                _hasWildcard = true
            case c@',' =>
                regex.append(if (curlyOpen > 0) '|' else c)
            case c@'}' =>
                if (curlyOpen > 0) { // end of a group
                    curlyOpen -= 1
                    regex.append(")")
                } else {
                    regex.append(c)
                }
            case c@'[' =>
                //if (setOpen > 0) GlobPattern.error("Unclosed character class", glob, i)
                setOpen += 1
                _hasWildcard = true
                regex.append(c)
            case c@'^' => // ^ inside [...] can be unescaped
                if (setOpen == 0) regex.append(BACKSLASH)
                regex.append(c)
            case '!' => // TODO: [! needs to be translated to [^
                regex.append('!')
            case c@']' =>
                // Many set errors like [][] could not be easily detected here,
                // as []], []-] and [-] are all valid POSIX glob and java regex.
                // We'll just let the regex compiler do the real work.
                setOpen -= 1
                regex.append(c)
            case c => regex.append(c)
        }
        //if (setOpen > 0) GlobPattern.error("Unclosed character class", glob, len)
        //if (curlyOpen > 0) GlobPattern.error("Unclosed group", glob, len)
        Pattern.compile(regex.toString, Pattern.DOTALL)
    }

    /**
      * @return the compiled pattern
      */
    def compiled: Pattern = _compiled

    /**
      * Match input against the compiled glob pattern
      *
      * @param s input chars
      * @return true for successful matches
      */
    def matches(s: CharSequence): Boolean = _compiled.matcher(s).matches

    /**
      * @return true if this is a wildcard pattern (with special chars)
      */
    def hasWildcard: Boolean = _hasWildcard
}
