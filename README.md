# SCD_Process
General process for SCD type 1 and SCD type 2 to change capture from MySQL and store in hive

MainOfSCD.scala
===============
This is a generalized process for importing any table from mysql to hive
using slowly changing dimension 1 or 2.

This program will take scd type '1' or '2' from the input args(0).

It takes the configurations from application.conf file. Any change to the names of database, tablename, primary key or change key should be done here.

The queries for SCD1 and SCD2 are in files, referenced appropriately in conf file.

configurations are mapped using a trait - configParms

update to hive for both scd are done using - hiveUpdateTrait called from MainOfSCD.scala


