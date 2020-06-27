# SF Crime Statistics with Spark Streaming

Created: Jun 26, 2020 8:52 AM

## **Project Overview**

In this project, you will be provided with a real-world dataset, extracted from Kaggle, on San Francisco crime incidents, and you will provide statistical analyses of the data using Apache Spark Structured Streaming. You will draw on the skills and knowledge you've learned in this course to create a Kafka server to produce data, and ingest data through Spark Structured Streaming.

You can try to answer the following questions with the dataset:

- What are the top types of crimes in San Fransisco?

- What is the crime density by location?

### **How to Run**
- Required servers and installations can be done using `sh start.sh`, this will start the required servers.
- Start the producer using `python producer_server.py`
- Start the spark job using `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 data_stream.py`

### **Output**
- The output of the project is attached in the `screenshots` directory.

### **Question 1**
- ```How did changing values on the SparkSession property parameters affect the throughput and latency of the data?```
    -  We can see change in`processedRowsPerSecond` (either increase or decrease in value). This number decides the throughput of the application.
### **Question 2**
- ```What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal? ```
    - spark.sql.shuffle.partitions
    - spark.streaming.kafka.maxRatePerPartition
    - spark.default.parallelism
    
    Parallelism is decided by total number of available cpus on the device, hence making more cpus available will increase the throughput i.e. `processedRowsPerSecond`
    
    Number of partitions are decided by the `total dataset size / partition size`, the optimal number of partitions allows less movemet of data during join/wide operations, which increases the throughput.
    
    Maximum Rate Per Partition sets the inflow rate of message per partition, tuning this will decrease the number of unprocessed messages which will lead to better performance.
    



### **Development Environment**

You may choose to create your project in the workspace we provide here, or if you wish to develop your project locally, you will need to set up your environment properly as described below:

- Spark 2.4.3
- Scala 2.11.x
- Java 1.8.x
- Kafka build with Scala 2.11.x
- Python 3.6.x or 3.7.x

### **Environment Setup (Only Necessary if You Want to Work on the Project Locally on Your Own Machine)**

### **For Macs or Linux:**

- Download Spark from **[https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)**. Choose "Prebuilt for Apache Hadoop 2.7 and later."
- Unpack Spark in one of your folders (I usually put all my dev requirements in /home/users/user/dev).
- Download binary for Kafka from this location **[https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)**, with Scala 2.11, version 2.3.0. Unzip in your local directory where you unzipped your Spark binary as well. Exploring the Kafka folder, you’ll see the scripts to execute in `bin` folders, and config files under `config` folder. You’ll need to modify `zookeeper.properties` and `server.properties`.
- Download Scala from the official site, or for Mac users, you can also use `brew install scala`, but make sure you download version 2.11.x.
- Run below to verify correct versions:

    ```
    java -version
    scala -version

    ```

- Make sure your ~/.bash_profile looks like below (might be different depending on your directory):

    ```
    export SPARK_HOME=/Users/dev/spark-2.4.3-bin-hadoop2.7
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home
    export SCALA_HOME=/usr/local/scala/
    export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SCALA_HOME/bin:$PATH

    ```

### **For Windows:**

Please follow the directions found in this helpful StackOverflow post: **[https://stackoverflow.com/questions/25481325/how-to-set-up-spark-on-windows](https://stackoverflow.com/questions/25481325/how-to-set-up-spark-on-windows)**

**[SF Crime Data](https://classroom.udacity.com/nanodegrees/nd029/parts/d3d2cbfa-dc05-44c3-8db8-84e1d931170d/modules/36ddbb88-7b71-4e78-983c-5a0890eb1ec2/lessons/9a62b3fd-3586-47a1-a1c9-5c727d4ec3b4/concepts/b8e5d5b1-6752-48be-a8de-a6ea4a63cfab#)**

![https://video.udacity-data.com/topher/2019/August/5d5198a2_screen-shot-2019-08-12-at-9.49.15-am/screen-shot-2019-08-12-at-9.49.15-am.png](https://video.udacity-data.com/topher/2019/August/5d5198a2_screen-shot-2019-08-12-at-9.49.15-am/screen-shot-2019-08-12-at-9.49.15-am.png)

SF Crime Data
