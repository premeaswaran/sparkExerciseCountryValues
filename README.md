Country-Values aggregation - Spark Exercise:
==

Question
--

Write an application using Python, Scala or Java that will use Spark to do the following:
1. Read the data file ‘data.csv’.
2. Create an optimised parquet file with the same data
3. Load the parquet file into Spark
4. Aggregate the values by country
5. Write the results to a parquet file

Input:
--

| Country  | Values         |
|----------|----------------|
| Canada   | 47;97;33;94;6  |
| Canada   | 59;98;24;83;3  |
| Canada   | 77;63;93;86;62 |
| China    | 86;71;72;23;27 |
| China    | 74;69;72;93;7  |
| China    | 58;99;90;93;41 |
| England  | 40;13;85;75;90 |
| England  | 39;13;33;29;14 |
| England  | 99;88;57;69;49 |
| Germany  | 67;93;90;57;3  |
| Germany  | 0;9;15;20;19   |
| Germany  | 77;64;46;95;48 |
| India    | 90;49;91;14;70 |
| India    | 70;83;38;27;16 |
| India    | 86;21;19;59;4  |

Expected Output:
--

| Country  | Values              |
|----------|---------------------|
| Canada   | 183;258;150;263;71  |
| China    | 218;239;234;209;75  |
| England  | 178;114;175;173;153 |
| Germany  | 144;166;151;172;70  |
| India    | 246;153;148;100;90  |

Steps Followed for the solution:
--
1. Extending the CountryValues class to App to use the 'main' of App Trait
2. Read the data.csv file as a dataframe
3. Write the dataframe as a parquet file
4. Read the parquet file and create a new dataframe
5. Convert the dataframe into an Array[Object]
6. Convert the array of object into String and separate out 'country' column as key
7. Convert the array of object into String and separate out 'values' column as value String
8. Split the value string at ';' and create an Array[Int] out of it
9. Create an empty map
10. Loop through the key and map the key-value pair as a map
11. If the key exists in the 'maps' already, add the value array
12. If the key does not exist in the 'maps' already, add the key and value to 'maps'
13. Convert the Array[Int] value of each key to a string separated by ';'
14. Convert the result of the mapping into a dataframe with two columns 'Country' and 'Values'
15. Write the dataframe into a Results.parquet file
