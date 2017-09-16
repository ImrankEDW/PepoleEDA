#Author: Imran
#Descri: Creating DF using struct

#from __future__ import printfunction;

import sys;

from pyspark.sql import SparkSession;

from pyspark.sql.types import StructType, StructField, StringType, IntegerType;



if __name__ == "__main__":
	
	if len(sys.argv) != 2:
		print("Usage: ReadDataFrameWithStruct.py data_filepath");
		exit(-1);

	filepath = sys.argv[1];

	spark = SparkSession.builder.appName("ReadDataFrameWithStruct").getOrCreate();
	strct = StructType([StructField("age", IntegerType(), True), 
	StructField("workclass", StringType(), True),
	StructField("finalweight", StringType(), True),
	StructField("education", StringType(), True),
	StructField("educationnum", StringType(), True),
	StructField("maritalstatus", StringType(), True),
	StructField("occupation", StringType(), True),
	StructField("relationship", StringType(), True),
	StructField("race", StringType(), True),
	StructField("gender", StringType(), True),
	StructField("capitalgain", StringType(), True),
	StructField("capitalloss", StringType(), True),
	StructField("hoursperweek", StringType(), True),
	StructField("country", StringType(), True),
	StructField("salary", StringType(), True)]);
	df = spark.read.csv(filepath, schema=strct);
	#df.take(2);
	df.show();
	spark.stop();

