# SqlDataFlow

An ETL tool based on sparksql and hocon(https://github.com/lightbend/config);

Write a configuration file to specify the ETL process.

step1. write the job conf file 

a. data from jdbc to cos(one kind cloud storage) sample:

```hocon

source {

 jdbc1 {
         tag=test
         table="(select * from test.test) as t"
         result_table_name=test
     }

}

sink{
      cos1 {
            tag=meta
            bucket=test
            table=test
            result_table_name="TEST.TEST"
            trim=true
      }
}

```
b. data from cos to dashdb, please refer the following :
```hocon
source {
    cos2 {
                useFullName=true
                tag=meta
                table="test.test"
                result_table_name=test
            }

     cos3 {
                      tag=meta
                      table="test.test"
                      fromSink=true
                      result_table_name=oldtest
                      targetDB=data
                      targetTable=test.test_new
           }
}

sink{
   dashdb5{
       metaTag=meta
       tag=data
       inNewView=test
       inOldView=oldtest
       batchSize= 10
       numPartitions= 1
       strategy= "Incremental"
       result_table_name= "test.test_new"
       priKeys = [
        { key=id }
        ]
   }
 }

```

you can get more samples from examples directory.

### How to deploy?
1.Assembly a fat jar
```bash
   mvn clean package -DskipTests -Pdefault
```

2.Copy job conf to cos bucket with cyberduck or minio client

3.Refer the following bash shell,save the following to runsdf.sh ,
usage: ./runsdf.sh env /path/to/job.conf
```bash 
#!/bin/bash

 if [ $# -lt 2 ]; then
   echo "usage runsdf.sh env /path/to/job.conf example: runsdf.sh vt cos://dsw-data-project-vt/dataflow/examples/jdbc2costest.conf"
   exit 1
 fi
appName=$(echo "$2" | sed 's|cos://.*/.*/.*/\(.*\)\.conf|\1|g'|sed 's/_//')
echo "$appName is start to run......"

env=$1

spark-submit \
--conf spark.kubernetes.namespace=spark-job \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--master k8s://https://master:port \
--deploy-mode cluster \
--name $appName  \
--class org.student.spark.Main \
--conf spark.executor.memory=4g \
--conf spark.driver.memory=4g \
--conf spark.executor.cores=1 \
--conf spark.executor.instances=4 \
--conf spark.kubernetes.driver.secrets.sdfsecret=/opt/spark/secrets/ \
--conf spark.kubernetes.container.image=student2021/spark:244 \
--driver-java-options \
SparkDataFlow-1.0.jar \
$2
```