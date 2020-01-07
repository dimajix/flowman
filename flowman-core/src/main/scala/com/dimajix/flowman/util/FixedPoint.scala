package com.dimajix.flowman.util

import scala.annotation.tailrec

object FixedPoint {
    def fix[T <: AnyRef](iter:T => T, stop:(T,T) => Boolean) : T => T = {
        @tailrec
        def recurse(x:T) : T = {
            val r = iter(x)
            if (stop(r, x))
                r
            else
                recurse(r)
        }
        recurse
    }
}
