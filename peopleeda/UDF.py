#Author: Imran
#Descri: Creating UDF 

#from __future__ import printfunction;

import sys;
from pyspark.sql import SparkSession;
from pyspark.sql.types import StructType, StructField, StringType, IntegerType;
from pyspark.sql.functions import udf,concat_ws,coalesce, lit;


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
	df.show();
	
	#Alternative 1: Using + Operator
	# dfcol = df.withColumn("newColumn", df.gender+ " --> " + df.maritalstatus);
	# dfcol.select(dfcol.newColumn).distinct().show();

	#Alternative 2: Using lambda/udf functions
	#concat_func = udf(lambda gender, maritalstatus: gender+"--"+maritalstatus);
	
	#Alternative3: Using comprehension, but replace N/A with null values
	def concat_func(*allColumns):
		return concat_ws(*[coalesce(eachColumn, lit("N/A")) for eachColumn in allColumns]);


	df = df.withColumn('gendermaritalstatus', combine('gender','maritalstatus'))
	df.select(df.gendermaritalstatus).distinct().show()

	spark.stop();
