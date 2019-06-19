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

