Spark Data Analysis Framework

To run this with the example code in Main.scala, simply type the following command
	
	sbt "run {path}" where 'path' is replaced with a *relative* path to the csv file.

Currently, we are using the Recidivism Normalized Dataset, so we can run the following command.

	sbt "run ../Datasets/ProPublica/RecidivismData_Normalized.csv"

The output should be placed in a file called  "output.csv"
