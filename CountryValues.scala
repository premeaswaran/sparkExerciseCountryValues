import spark.implicits._
import org.apache.spark.rdd.RDD

public class CountryValues extends App{

	val df = spark.read.option("header","true").csv("data.csv")
	df.write.parquet("read_modified.parquet")
	val df1 = spark.read.option("header","true").parquet("read_modified.parquet")
	val arr1 = df1.collectAsList().toArray()
	
	val key = arr1.map(a => a.toString().substring(1,a.toString().indexOf(",")))
	val vals = arr1.map(a => a.toString().substring(a.toString().indexOf(",")+1, a.toString().indexOf("]")))
	val valsArr = vals.map(v => v.split(";").map(_.toInt))
	
	var maps = Map[String, Array[Int]]()
	for(i <- 0 until key.length) {
		if(maps.contains(key(i))) {
			maps += (key(i) -> (valsArr(i),maps(key(i))).zipped.map(_+_))
		} else {
			maps += (key(i) -> (valsArr(i)))
		}
	}
	
	var modVal = maps.map(x => (x._1, (x._2(0) + ";" + x._2(1) + ";" + x._2(2) + ";" + x._2(3) + ";" + x._2(4))))
	val result = modVal.toSeq.toDF("Country","Values")
	
	result.write.format("parquet").save("Result.parquet")
}