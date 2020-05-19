package ca.mcit.bigdata.hive

import java.sql.{Connection, DriverManager}

object enricher extends App {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/fall2019_surya","cloudera","cloudera")
  val stmt = connection.createStatement()
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")

  stmt.execute("set hive.exec.dynamic.partition=true")

  stmt.execute("drop table IF EXISTS fall2019_surya.enriched_trip")

  stmt.execute("create table fall2019_surya.enriched_trip ( " +
    "route_id Int,         " +
    "service_id String,   " +
    "trip_id String,      " +
    "trip_headsign String," +
    "direction_id Int,    " +
    "shape_id Int,        " +
    "note_fr Int,      " +
    "note_en Int,      " +
    "date Int,          " +
    "exception_type Int," +
    "start_time String,    " +
    "end_time String,      " +
    "headway_sec Int)" +

    "PARTITIONED BY (wheelchair_accessible Int)" +
    "TBLPROPERTIES('parquet.compression'='GZIP')")

  stmt.execute("Insert overwrite table fall2019_surya.enriched_trip PARTITION(wheelchair_accessible) " +
    "select A.route_id ,A.service_id , A.trip_id,A.trip_headsign,A.direction_id ,A.shape_id , A.note_fr ,A.note_en , B.date ,B.exception_type ,C.start_time , C.end_time ,C.headway_sec,A.wheelchair_accessible " +
    "from fall2019_surya.ext_trips A " + "left join fall2019_surya.ext_calendar_dates B on A.service_id = B.service_id " +
    "left join fall2019_surya.ext_frequencies C on A.trip_id = C.trip_id")

  stmt.close()
  connection.close()
}

