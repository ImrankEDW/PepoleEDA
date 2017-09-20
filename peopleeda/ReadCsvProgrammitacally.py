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

	sc = spark.sparkContext; #manually init sparkcontext

	lines = sc.textFile(filepath); #/* Reading it as text file and convert each line into row*/
	parts = lines.map(lambda l: l.split(","));# Each line is converted to a tuple.
	people = parts.map(lambda p: (p[0].strip(), p[1].strip())); #manually trimming strings

	schemaString = "age workclass";

	fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()];

	schema = StructType(fields);
	schemaPeople = spark.createDataFrame(people, schema);

    schemaPeople.createOrReplaceTempView("people")
    results = spark.sql("SELECT * FROM people")

	results.show()

	spark.stop();
