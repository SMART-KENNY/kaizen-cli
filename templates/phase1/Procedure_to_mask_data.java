##### IF YOUR SAMPLE FILE DELIMITED FILE ######

/home/talino/talino-platform/bin/karding-devel-wealthy.sh adhoc
import scala.io.Source
sc.hadoopConfiguration.set("mapreduce.output.fileoutputformat.compress", "false")


val txt_mapping = "<local_file_path>"
val <pipeline> = Source.fromFile(txt_mapping).getLines().map { line =>
  val Array(index, fieldName) = line.split(",")
  (index.toInt, fieldName)
}.toList
val <pipeline>_indices = <pipeline>.map(_._1)
val <pipeline>_fieldNames = <pipeline>.map(_._2)
val <pipeline>_dataFilePath = "<soure_file_path>"
val <pipeline>_rawData = sc.textFile(<pipeline>_dataFilePath)
val <pipeline>_data = <pipeline>_rawData.map(line => line.split("<delimeter>"))
val <pipeline>_selectedColumns = <pipeline>_data.map(columns => {
  val selected = <pipeline>_indices.map(index => if (index < columns.length && columns(index).nonEmpty) columns(index) else "")
  Row(selected: _*)
})

val <pipeline>_schema = StructType(<pipeline>_fieldNames.map(fieldName => StructField(fieldName, StringType, true)))
val <pipeline>_df = sqlContext.createDataFrame(<pipeline>_selectedColumns, <pipeline>_schema)
<pipeline>_df.registerTempTable("<pipeline>")


val output = sql("select <sql_fields_mask> from <pipeline> limit 20")
val repartitionedData = output.repartition(1)
repartitionedData.map(row=>row.mkString("<delimeter>")).saveAsTextFile("<target_file_path>")



-------------------------------------------------------------------------------------------------------------------------------------------------



###### USE BELOW IF YOUR SAMPLE FILE PARQUET FILE ######

val <pipeline>_file = sqlContext.parquetFile("<soure_file_path>/")
<pipeline>_file.registerTempTable("<pipeline>")

val output = sql("select <sql_fields_mask> from <pipeline> limit 20")
val repartitionedData = output.repartition(1)
repartitionedData.write.parquet("<target_file_path>")