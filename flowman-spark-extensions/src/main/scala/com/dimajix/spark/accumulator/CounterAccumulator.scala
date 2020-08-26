package com.dimajix.spark.accumulator

import scala.collection.mutable

import org.apache.spark.util.AccumulatorV2


class CounterAccumulator() extends AccumulatorV2[String,Map[String, Long]]{

  private val counters = mutable.Map[String, Long]().withDefaultValue(0)

  /**
    * Returns true if this accumulator is zero, i.e. if it doesn't contain any values
    * @return
    */
  override def isZero: Boolean = {
    counters.synchronized {
      counters.keySet.isEmpty
    }
  }

  /**
    * Creates a copy of this accumulator
    * @return
    */
  override def copy: CounterAccumulator = {
    val newAccumulator = new CounterAccumulator()
    counters.synchronized {
      for ((key, value) <- counters) {
        newAccumulator.counters.update(key, value)
      }
    }
    newAccumulator
  }

  /**
    * Resets this accumulator to its zero value
    */
  override def reset: Unit = {
    counters.clear()
  }

  /**
    * Adds a new value to this accumulator. This will increase the counter of the specified name
    * @param name
    */
  override def add(name: String): Unit = {
    counters.synchronized {
      counters.update(name, counters(name) + 1)
    }
  }

  /**
    * Adds a whole map of key-value pairs. Since this requires a single synchronisation section, this will be
    * faster than calling multiple single add methods sequentially
    * @param values
    */
  def add(values: Map[String,Long]): Unit = {
    counters.synchronized {
      for ((key, value) <- values) {
        counters.update(key, counters(key) + value)
      }
    }
  }

  /**
    * Merges in the values of another accumulator
    * @param otherAccumulator
    */
  override def merge(otherAccumulator: AccumulatorV2[String,Map[String, Long]]): Unit = {
    val otherCounters = otherAccumulator.value
    counters.synchronized {
      for ((key, value) <- otherCounters) {
        counters.update(key, counters(key) + value)
      }
    }
  }

  /**
    * Returns the current value of this accumulator
    * @return
    */
  override def value: Map[String, Long] = {
    counters.synchronized {
      counters.toMap
    }
  }

  /**
    * Returns the counter for a single name. If no information is available, None will be returned
    * @param name
    * @return
    */
  def get(name:String) : Option[Long] = {
    counters.synchronized {
      counters.get(name)
    }
  }
}
