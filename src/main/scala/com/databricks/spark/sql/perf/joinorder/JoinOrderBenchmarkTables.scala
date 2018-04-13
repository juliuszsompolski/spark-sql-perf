/*
 * Copyright 2018 Databricks Inc.
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

import com.databricks.spark.sql.perf.{TableDDL, TablesDDL}
import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types._

class JoinOrderBenchmarkTables(sqlContext: SQLContext, csvDataDir: String)
    extends TablesDDL(sqlContext) {

  private val log = LoggerFactory.getLogger(getClass)

  case class Table(override val name: String, fields: StructField*)
      extends TableDDL(sqlContext, name, Seq.empty, fields) {

    private val log = LoggerFactory.getLogger(getClass)

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        coalesce: Boolean = true): Unit = {

      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
      val data = sqlContext.read.format("csv").schema(schema).load(s"${csvDataDir}/${name}.csv")
      val writer = if (coalesce) {
        data.coalesce(1).write
      } else {
        data.write
      }
      writer.format(format).mode(mode)
      println(s"Generating table $name in database to $location with save mode $mode.")
      log.info(s"Generating table $name in database to $location with save mode $mode.")
      writer.save(location)
    }
  }

  def genData(
    location: String,
    format: String,
    overwrite: Boolean,
    coalesce: Boolean = true,
    tableFilter: String = ""): Unit = {
    val tablesToBeGenerated = if (!tableFilter.isEmpty) {
      tables.filter(_.name == tableFilter)
    } else {
      tables
    }

    if (tablesToBeGenerated.isEmpty) {
      throw new RuntimeException("Bad table name filter: " + tableFilter)
    }

    tablesToBeGenerated.foreach { table =>
      val tableLocation = s"$location/${table.name}"
      table.genData(tableLocation, format, overwrite, coalesce)
    }
  }

  import sqlContext.implicits._

  override val tables = Seq(
    Table("aka_name",
      StructField("id", IntegerType, false),
      StructField("person_id", IntegerType, false),
      StructField("name", StringType, false),
      'imdb_index.string,
      'name_pcode_cf.string,
      'name_pcode_nf.string,
      'surname_pcode.string,
      'md5sum.string
    ),
    Table("aka_title",
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("title", StringType, false),
      'imdb_index.string,
      StructField("kind_id", IntegerType, false),
      'production_year.int,
      'phonetic_code.string,
      'episode_of_id.int,
      'season_nr.int,
      'episode_nr.int,
      'note.string,
      'md5sum.string
    ),
    Table("cast_info",
      StructField("id", IntegerType, false),
      StructField("person_id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      'person_role_id.int,
      'note.string,
      'nr_order.int,
      StructField("role_id", IntegerType, false)
    ),
    Table("char_name",
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      'imdb_index.string,
      'imdb_id.int,
      'name_pcode_nf.string,
      'surname_pcode.string,
      'md5sum.string
    ),
    Table("comp_cast_type",
      StructField("id", IntegerType, false),
      StructField("kind", StringType, false)
    ),
    Table("company_name",
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      'country_code.string,
      'imdb_id.int,
      'name_pcode_nf.string,
      'name_pcode_sf.string,
      'md5sum.string
    ),
    Table("company_type",
      StructField("id", IntegerType, false),
      StructField("kind", StringType, false)
    ),
    Table("complete_cast",
      StructField("id", IntegerType, false),
      'movie_id.int,
      StructField("subject_id", IntegerType, false),
      StructField("status_id", IntegerType, false)
    ),
    Table("info_type",
      StructField("id", IntegerType, false),
      'info.string
    ),
    Table("keyword",
      StructField("id", IntegerType, false),
      StructField("keyword", StringType, false),
      'phonetic_code.string
    ),
    Table("kind_type",
      StructField("id", IntegerType, false),
      StructField("kind", StringType, false)
    ),
    Table("link_type",
      StructField("id", IntegerType, false),
      StructField("link", StringType, false)
    ),
    Table("movie_companies",
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("company_id", IntegerType, false),
      StructField("company_type_id", IntegerType, false),
      'note.string
    ),
    Table("movie_info",
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("info_type_id", IntegerType, false),
      'info.string,
      'note.string
    ),
    Table("movie_info_idx",
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("info_type_id", IntegerType, false),
      StructField("info", StringType, false),
      'note.string
    ),
    Table("movie_keyword",
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("keyword_id", IntegerType, false)
    ),
    Table("movie_link",
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("linked_movie_id", IntegerType, false),
      StructField("link_type_id", IntegerType, false)
    ),
    Table("name",
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      'imdb_index.string,
      'imdb_id.int,
      'gender.string,
      'name_pcode_cf.string,
      'name_pcode_nf.string,
      'surname_pcode.string,
      'md5sum.string
    ),
    Table("person_info",
      StructField("id", IntegerType, false),
      StructField("person_id", IntegerType, false),
      StructField("info_type_id", IntegerType, false),
      StructField("info", IntegerType, false),
      'note.string
    ),
    Table("role_type",
      StructField("id", IntegerType, false),
      StructField("role", StringType, false)
    ),
    Table("title",
      StructField("id", IntegerType, false),
      StructField("title", StringType, false),
      'imdb_index.string,
      StructField("kind_id", IntegerType, false),
      'production_year.int,
      'imdb_id.int,
      'phonetic_code.string,
      'episode_of_id.int,
      'season_nr.int,
      'episode_nr.int,
      'series_years.string,
      'md5sum.string
    )
  )
}
