import spark.implicits._

public class CountryValues extends App{
	val df = spark.read.option("header","true").csv("data.csv")
	df.write.parquet("read_modified.parquet")
	val df1 = spark.read.option("header","true").parquet("read_modified.parquet")
  
  	case class Entries(country: String, values: Array[Int])
	val ds = df1.withColumn("Values", split(col("Values"),";").cast("array<int>")).as[Entries]
  
  	val n = ds.select(size('Values)).first.getAs[Int](0)
	val summed = ds.groupBy("Country").agg(array((0 until n).map(i => sum(col("Values").getItem(i))) :_* ) as "Values")
	val result = summed.map(a => (a.getString(0), a.getSeq(1).toArray.mkString(";"))).toDF("Country","Values")
	
  	result.write.format("parquet").save("Result.parquet")
}
