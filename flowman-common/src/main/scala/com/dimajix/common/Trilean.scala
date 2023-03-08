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

package com.dimajix.common

sealed abstract class Trilean {
    def unary_! : Trilean
    def ||(other:Trilean) : Trilean
    def &&(other:Trilean) : Trilean
}

case object Yes extends Trilean {
    override def toString: String = "yes"
    override def unary_! : Trilean = No
    override def ||(other:Trilean) : Trilean = this
    override def &&(other:Trilean) : Trilean = other
}
case object No extends Trilean {
    override def toString: String = "no"
    override def unary_! : Trilean = Yes
    override def ||(other:Trilean) : Trilean = other
    override def &&(other:Trilean) : Trilean = this
}
case object Unknown extends Trilean {
    override def toString: String = "unknown"
    override def unary_! : Trilean = Unknown
    override def ||(other:Trilean) : Trilean = {
        if (other == Yes)
            other
        else
            this
    }
    override def &&(other:Trilean) : Trilean =  {
        if (other == No)
            other
        else
            this
    }
}


object Trilean {
    implicit def toTrilean(b:Boolean) : Trilean = {
        if (b) Yes else No
    }
}
