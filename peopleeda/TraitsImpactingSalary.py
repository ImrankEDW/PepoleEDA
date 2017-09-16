#Author: Imran
#Descri: Count group by salary

#from __future__ import printfunction;

import sys;
from pyspark.sql import SparkSession;
from pyspark.sql.types import StructType, StructField, StringType, IntegerType;
from pyspark.sql.functions import regexp_replace;
from pyspark.sql.functions import rtrim;
from pyspark.sql.functions import ltrim;


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


	dfregex1 = df.select(df.workclass, df.finalweight, df.education, df.educationnum, df.maritalstatus, df.occupation, df.relationship
		, df.race , df.gender, df.capitalgain, df.capitalloss, df.hoursperweek, df.country, regexp_replace(df.salary, '(<=50K)', '50').alias('salary'));


	dfregex = dfregex1.select(dfregex1.workclass, dfregex1.finalweight, dfregex1.education, dfregex1.educationnum, dfregex1.maritalstatus, dfregex1.occupation
		, dfregex1.relationship, dfregex1.race , dfregex1.gender, dfregex1.capitalgain, dfregex1.capitalloss, dfregex1.hoursperweek, dfregex1.country
		, regexp_replace(dfregex1.salary, '(>50K)', '51').alias('salary'));

	dfbtrim = dfregex1.select(dfregex1.workclass, dfregex1.finalweight, dfregex1.education, dfregex1.educationnum, dfregex1.maritalstatus
		, dfregex1.occupation, dfregex1.relationship, dfregex1.race , dfregex1.gender, dfregex1.capitalgain, dfregex1.capitalloss
		, dfregex1.hoursperweek, dfregex1.country, ltrim(rtrim(dfregex1.salary)).alias('salary') );


	dfcast = dfbtrim.select(dfbtrim.workclass, dfbtrim.finalweight, dfbtrim.education, dfbtrim.educationnum, dfbtrim.maritalstatus, dfbtrim.occupation
		, dfbtrim.relationship, dfbtrim.race , dfbtrim.gender, dfbtrim.capitalgain, dfbtrim.capitalloss, dfbtrim.hoursperweek, dfbtrim.country
		, dfbtrim.salary.cast(IntegerType()).alias('intSal'));

	dfcast.createOrReplaceTempView("employees");

	query = "select workclass, education, maritalstatus, occupation, relationship, hoursperweek, country, avg(intSal) as sal_avg from employees " +
		"group by workclass, education, maritalstatus, occupation, relationship, hoursperweek, country";

	sqldf = spark.sql(query);
	sqldf.dropna().show(10);

	spark.stop();



	
