# Python_athena_to_mysql_ingestion
This repository has a python code which queries data from an Athena source and, retrieving a mysql database info from a Dynamodb, inserts the data into the Mysql table.

In the process, the process is done in a loop, going from today's date minus X days, to today's date minus Y.
Code uses python's awswrangler library.
