# MapReduce
Implementation of MapReduce programs for self learning.Programs other than word count.

**Program 1**: Calculates the average price of the Shares present in HCLTECH.csv file.Place the HCLTECH.csv file in your HDFS and run the below code.Make use of jar file present in Program1 folder:
hadoop jar MapReduce.jar pack1.Program1 input-directory-path output-directory-path

**Program 2**: Prints the values of dates and prices where the Shares price present in HCLTECH.csv file is more than 1000rs.Place the HCLTECH.csv file in your HDFS and run the code .Make use of jar file present in Program2 folder:
hadoop jar MapReduce.jar pack1.Program2 input-directory-path output-directory-path

**Program 3**: Calculates the word count in the Sample.txt file.Place the sample.txt file in your HDFS and run the code .Make use of jar file present in Program3 folder:
hadoop jar MapReduce.jar pack1.Program3 input-directory-path output-directory-path

**Program 4**: This makes use of implementation Writeable interface.Stock.csv is the file which is used .It will store the values where the Stock High price is more than 5 and output will contain Key=Date and Value="Stock details as in Price,Symbol,Exchange etc".Make use of Map Reduce jar present in Program4 folder
hadoop jar MapReduce.jar pack1.Program3 input-directory-path output-directory-path
