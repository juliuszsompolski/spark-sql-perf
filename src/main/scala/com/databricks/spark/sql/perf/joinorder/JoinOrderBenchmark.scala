/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.joinorder

import org.apache.commons.io.IOUtils
import com.databricks.spark.sql.perf.{Benchmark, ExecutionMode, Query}

import org.apache.spark.sql.SQLContext

/**
 * This implements Join Order Benchmark queries from http://www.vldb.org/pvldb/vol9/p204-leis.pdf
 */
class JoinOrderBenchmark(sqlContext: SQLContext)
  extends Benchmark(sqlContext) {

  import ExecutionMode._

  private val queryNames = Seq(
    "1a", "1b", "1c", "1d",
    "2a", "2b", "2c", "2d",
    "3a", "3b", "3c",
    "4a", "4b", "4c",
    "5a", "5b", "5c",
    "6a", "6b", "6c", "6d", "6e", "6f",
    "7a", "7b", "7c",
    "8a", "8b", "8c", "8d",
    "9a", "9b", "9c", "9d",
    "10a", "10b", "10c",
    "11a", "11b", "11c", "11d",
    "12a", "12b", "12c",
    "13a", "13b", "13c", "13d",
    "14a", "14b", "14c",
    "15a", "15b", "15c", "15d",
    "16a", "16b", "16c", "16d",
    "17a", "17b", "17c", "17d", "17e", "17f",
    "18a", "18b", "18c",
    "19a", "19b", "19c", "19d",
    "20a", "20b", "20c",
    "21a", "21b", "21c",
    "22a", "22b", "22c", "22d",
    "23a", "23b", "23c",
    "24a", "24b",
    "25a", "25b", "25c",
    "26a", "26b", "26c",
    "27a", "27b", "27c",
    "28a", "28b", "28c",
    "29a", "29b", "29c",
    "30a", "30b", "30c",
    "31a", "31b", "31c",
    "32a", "32b",
    "33a", "33b", "33c"
  )

  val joinOrderBenchmarkQueries = queryNames.map { queryName =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"joinorder/$queryName.sql"))
    Query(queryName, queryContent, description = "Join Order Benchmark Query",
      executionMode = CollectResults)
  }

  val joinOrderBenchmarkQueriesMap = joinOrderBenchmarkQueries.map(q => q.name -> q).toMap
}
