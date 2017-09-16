#Author: Imran
#Descri: count no of distinct peoples 

#from __future__ import printfunction;

import sys;
from pyspark.sql import SparkSession;


if __name__ == "__main__":
	
	if len(sys.argv) != 2:
		print("Usage: PeopleCountBetweenSal.py data_filepath");
		exit(-1);

	data_filepath = sys.argv[1];

	spark = SparkSession.builder.appName("PeopleWordCount").getOrCreate();

	# cReate view from sparkSession (spark)

	df = spark.read.csv(data_filepath);
	count = df.distinct().count();
	print "No of distinct rows = ",count; 
	spark.stop();

