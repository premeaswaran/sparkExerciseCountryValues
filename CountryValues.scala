import spark.implicits._
import org.apache.spark.rdd.RDD

public class CountryValues extends App{       //Extending the CountryValues class to App to use the 'main' of App Trait

val df = spark.read.option("header","true").csv("data.csv")	//Read the data.csv file as a dataframe
df.write.parquet("read_modified.parquet") //Write the dataframe as a parquet file
val df1 = spark.read.option("header","true").parquet("read_modified.parquet")	//Read the parquet file and create a new dataframe
val arr1 = df1.collectAsList().toArray()	//convert the dataframe into an Array[Object]

val key = arr1.map(a => a.toString().substring(1,a.toString().indexOf(",")))	//Convert the array of object into String and separate out 'country' column as key
val vals = arr1.map(a => a.toString().substring(a.toString().indexOf(",")+1, a.toString().indexOf("]")))	//Convert the array of object into String and separate out 'values' column as value String
val valsArr = vals.map(v => v.split(";").map(_.toInt))	//Split the value string at ';' and create an Array[Int] out of it

var maps = Map[String, Array[Int]]()	//Create an empty map
for(i <- 0 until key.length) {	//Loop through the key and map the key-value pair as a map
if(maps.contains(key(i))) {
maps += (key(i) -> (valsArr(i),maps(key(i))).zipped.map(_+_))	//if the key exists in the 'maps' already, add the value array
} else {
maps += (key(i) -> (valsArr(i)))	//if the key does not exist in the 'maps' already, add the key and value to 'maps'
}
}

var modVal = maps.map(x => (x._1, (x._2(0) + ";" + x._2(1) + ";" + x._2(2) + ";" + x._2(3) + ";" + x._2(4))))	//Convert the Array[Int] value of each key to a string separated by ';'
val result = modVal.toSeq.toDF("Country","Values")	//convert the result of the mapping into a dataframe with two columns 'Country' and 'Values'

result.write.format("parquet").save("Result.parquet")	//Write the dataframe into a Results.parquet file
}
