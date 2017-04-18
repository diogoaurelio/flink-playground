### Intro

A Flink application project using Scala and SBT.

To run and test your application use SBT invoke: 'sbt run'

In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.


### Flink setup for hadoop & S3 



#### Add Flink conf as environment variable

To run in intellij, set flink configuration directory as an environmental variable:

FLINK_CONF_DIR=/home/<user>/flink-config


### Common issues
 
1) Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/flink/api/common/ExecutionConfig$GlobalJobParameters
This is probably due to the fact that you are running from intellij/terminal, and do not have all classes loaded into your class path.
In order to run your application from within IntelliJ, you have to select the classpath of the 'mainRunner' module in the run/debug configurations. Simply open 'Run -> Edit configurations...' and then select 'mainRunner' from the Use classpath of module dropbox.

2) Caused by: java.io.IOException: No file system found with scheme s3, referenced in file URI 's3://YOUR-BUCKET-HERE/flink-basic-read-from-s3.txt'.
This is probably due to the fact that you have not specified your hadoop conf in environment variables. go ahead and add environment variable 
HADOOP_CONF_DIR=/PATH-TO-THIS-REPO/playground/src/main/resources/hadoop-config


3) Caused by: java.lang.NoSuchMethodError: com.amazonaws.http.AmazonHttpClient.disableStrictHostnameVerification()V
Most 
