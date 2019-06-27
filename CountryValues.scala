import spark.implicits._
import org.apache.spark.rdd.RDD

public class CountryValues extends App{
  //read the file
	val df = spark.read.option("header","true").csv("data.csv")
	df.write.parquet("read_modified.parquet")
	val df1 = spark.read.option("header","true").parquet("read_modified.parquet")
  
  //creating a case class and a dataset off the case class and extract country and values as keys and values
  case class Entries(country: String, values: Array[Int])
	val ds = df1.withColumn("Values", split(col("Values"),";").cast("array<int>")).as[Entries]
	val keyArr = ds.select("country").as[String].collect()
	val valsArr = ds.select("values").as[Array[Int]].collect()
  
  //declare two empty maps
  var maps = Map[String, Array[Int]]()
	var mapsFinal = Map[String, String]()
	
  //custom function to sum the array of integer values element-wise and store into a map
  def sumArr(s:String, ar:Array[Int]) {
		if(maps.contains(s)) { maps += (s -> (ar,maps(s)).zipped.map(_+_)); }
		else { maps += (s -> ar);	}
	}
	
  //use custom function and sum the values element-wise and convert to string separated by ";"
  (keyArr zip valsArr).foreach(a => sumArr(a._1,a._2))
	maps.foreach(b => (mapsFinal += (b._1 -> b._2.mkString(";"))))
	val result =  mapsFinal.toSeq.toDF("Country", "Values")
	
  //write to parquet file
  result.write.format("parquet").save("Result.parquet")
}
