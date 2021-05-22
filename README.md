MapReduce source files are located in src/main/java/. 

You can host the compiled Java files in a Hadoop installed environment, and set up the HDFS master-slave topology to test out MapReduce. 

I use an Docker image to set up three Hadoop containers, one acts as a master and the other two act as slaves, to emulate the HDFS master slave topology for MapReduce.
