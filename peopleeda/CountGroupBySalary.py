#Author: Imran
#Descri: Count group by salary

#from __future__ import printfunction;

import sys;
from pyspark.sql import SparkSession;
from pyspark.sql.types import StructType, StructField, StringType, IntegerType;
from pyspark.sql.functions import regexp_replace;


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


	# Use regexp_replace function to replace symbols <= or > 

	dfregex1 = df.select(df.workclass, df.finalweight, df.education, df.educationnum, df.maritalstatus, df.occupation
		, df.relationship, df.race , df.gender, df.capitalgain, df.capitalloss, df.hoursperweek, df.country
		, regexp_replace(df.salary, '(<=50K)', '50').alias('salary'));

	dfregex = dfregex1.select(dfregex1.workclass, dfregex1.finalweight, dfregex1.education, dfregex1.educationnum
		, dfregex1.maritalstatus, dfregex1.occupation, dfregex1.relationship, dfregex1.race , dfregex1.gender, dfregex1.capitalgain
		, dfregex1.capitalloss, dfregex1.hoursperweek, dfregex1.country, regexp_replace(dfregex1.salary, '(>50K)', '51').alias('salary'));

	dfregex.select(dfregex.salary).distinct().show();

	dfregex.groupBy('salary').agg({'salary': 'count'}).collect();

	spark.stop();
