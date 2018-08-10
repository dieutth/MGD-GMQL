# Multidimensinal Genomic Data Representation

## 1. Run the Experiments
The experiments can be executed either by running the project in an IDE or by submitting a the project jar file to Spark.

### 1. Run experiments from IDE
To run the project from an IDE, simply import the newly cloned project to a suitable IDE (IntelliJ recommended). The code can be run from the `main` function in `de.tub.dima.G_BENCHMARKING.G_LocalBenchmarking.scala`

### 2. Run experiments by submitting a Spark job

#### 2.1. Build jar file to submit to Spark
The following code snippet shows how to create a fat jar file from the project using maven.
```sh
$ git clone https://github.com/dieutth/MGD-GMQL.git
$ cd MGD-GMQL
$ git checkout cluster
$ mvn clean package
```
The last command `mvn clean package` (which might take some time to finish) will create the jar file `uber-MGD-GMQL-1.0-SNAPSHOT.jar` in folder `path/to/MGD-GMQL/target/`. This file will be used to submit to Spark to run the experiments.
#### 2.2. Script to submit the job to Spark
The following code snippet shows a general script to submit to the jar file to Spark. For more information, see https://spark.apache.org/docs/latest/submitting-applications.html. 
```sh
/path/to/spark/bin/folder/spark-submit 
--class de.tub.dima.G_BENCHMARKING.G_ClusterBenchmarking 
--master <master-url>
--conf spark.network.timeout=timeout 
--conf spark.default.parallelism=parallelism
/path/to/MGD-GMQL/target/uber-MGD-GMQL-1.0-SNAPSHOT.jar
argument_1 otherArgs
```

The list of arguments is summarized in the following table.
//TODO

### 2.3. Script Examples
The following code snippet shows an example of submitting a job to Spark using `cluster mode` to perform a MAP query, parallelism is 288, using data in the flatten representation (i.e. running query using current system) between 2 datasets *HG19_BED_ANNOTATION* and *narrow*.
```sh
/share/hadoop/dieutth/spark-2.3.0-bin-hadoop2.7/bin/spark-submit 
--class de.tub.dima.G_BENCHMARKING.G_ClusterBenchmarking 
--master spark://ibm-power-1.dima.tu-berlin.de:7077 
--conf spark.network.timeout=10000s 
--conf spark.default.parallelism=288 
/home/dhong/MGD-GMQL/target/uber-MGD-GMQL-1.0-SNAPSHOT.jar
current_map 
/share/hadoop/dieutth/data/HG19_BED_ANNOTATION 
/share/hadoop/dieutth/data/narrow/
```
The following code snippet  shows an example of submitting a job to Spark using `standalone mode` (i.e running locally) to perform JOIN query, parallelism is 4, using data in the single-matrix-based representation (i.e. running the query with the new implementation) between 2 datasets *TSS* and *Gene_Promoter*
```sh
/home/dieutth/installer/spark-2.3.0-bin-hadoop2.7/bin/spark-submit 
--class de.tub.dima.G_BENCHMARKING.G_ClusterBenchmarking 
--master spark://local:8080
--conf spark.network.timeout=10000s 
--conf spark.default.parallelism=4 
/home/dieutth/gitworkspace/MGD-GMQL/target/uber-MGD-GMQL-1.0-SNAPSHOT.jar
arrarr_join_nc 
/home/dieutth/data/gmql/TSS
/home/dieutth/data/gmql/Gene_Promoter
```
