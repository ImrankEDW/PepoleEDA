#Author: Imran
#Descri: Creating tuple by using spark.read.json(jsonString)




import sys;
from pyspark.sql import SparkSession;
from collections import namedtuple;
#from pyspark.mllib.fpm import namedtuple

if __name__ == "__main__":
	
	if len(sys.argv) != 2:
		print("Usage: NamedTuple.py data_filepath");
		exit(-1);

	filepath = sys.argv[1];

	spark = SparkSession.builder.appName("CreateNamedTupleFromJsonString").getOrCreate();

	jstr = {"age", "workclass", "finalweight", "education", "educationnum", "maritalstatus", "occupation", "relationship", "race", "gender", "capitalgain", "capitalloss", "hoursperweek", "country", "salary"};

	Employee = namedtuple('Employee', sc.parallelize(jstr).collect()); #collect returns list 

	def eachline_to_employee(line):
		cell = line.split(", ");
		return Employee(int(cell[0]), cell[1], cell[2], cell[3], cell[4], cell[5], cell[6], cell[7], cell[8], cell[9], cell[10], cell[11], cell[12], cell[13], cell[14]);

	rdd = sc.textFile(filepath);
	emp_rdd = rdd.map(eachline_to_employee);
	empdf = emp_rdd.toDF();

	empdf.schema;
	empdf.describe;

	empdf.show(1);

	spark.stop();