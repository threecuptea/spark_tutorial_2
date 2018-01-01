### spark-tutorial_2 collects small projects that I worked on using spark scala 2 Dataset/ DataFrame.  Those were first inpired by open source articles from Databricks community, LinkedIn 'Apache Spark' User Group.  I instilled my idea while rewriting. 
#### The topics include:
    1. A complete movie recommendation System using Spark-ML ALS (Alternating Least Square) algorithm 
       a) Use original format of MovieLens 1M Dataset (https://grouplens.org/datasets/movielens/1m/)
       b) Write python scripts to generate my PersonalRating data
       c) Parse data into Rating and Movie case classes
       d) Adopt the strategy learned from edx.org BerkeleyX course "CS120x: Distrubuted Machine Learning with Apache 
          Spark": split data into training, validation and test dataset.  Use the best model evaluated based upon 
          validation dataset and apply it to test dataset
       e) Use ParamGridBuilder to faciliate GridSearch to find best model
       f) Compare RMSE from the prediction of best model with the baseline's one which use average rating as the 
          prediction
       g) Augment the model with all dataset and perform transform on unrated movies
       h) Unrated dataset was first generated by excluding movieId of PersonRating from all MovieRatings then join with 
          Movie. Improve performance by using Movies dataset directly to avoid expensive join plus distinct function.
          The first approach is reflected in recommend.log and the second approach is reflect in the recommend2.log.
          The second approach definitely perform better.
          
       i) Avoid NaN pitfall by excluding NaN prediction data from the result from transformation.  
          See MovieLensALS.scala for the details of NaN pitfall. 
       j) The remaining issue: accented characters in title of movies like è in La Vita è bella is lost in Spark 
          DataFrame operation. Research on org.apache.spark.sql.Encoder might be needed.
 
    2. Similar movie recommendation system but the source is data in MongoDB
       a) Write simple convert.py to covert the delimiter from '::' to ',' so that I can use mongoimport to import 
          Ratings and Movies data
       b) Use MongoSpark and ReadConfig of mongo-spark-connector 2.0 to load Mongodb data
       c) Perform similar steps as the above ML-1M recommendation system.
       d) Save recommendations to mongo

    3. Analyze Apache access log
       a) Write AccessLogParser using RegEx pattern to parse data into AccessLogRecord 
       b) Write parseDate UDF to parse access log date format to be compatible with timestamp type 
       c) Load diamond.csv using new SparkSession.read.csv with correct options 
       d) DataFrame Join of access log data with diamond.csv
    
    4. DanubeStatesAnalysis,DanubeStatesAnalysis2
       A task for Rovi to ensure the java-transform system is compatible with Pri-java-transform system by analyzing 
       transformer.log entries, including parsing, filter data by pubId and grouping.
       a) DanubeStatesAnalysis has separate non java-transform Dataset and java-transform Dataset and group them and 
          generate reports separately.  However, it's difficult to compare separate reports grouped by resources since
          there are too many resources.
       b) DanubeStatesAnalysis2 improves on top of DanubeStatesAnalysis. It parses non java-transform and java-transform 
          into a common obeject: DanubeStates which has additional numeric fields jtNo and jtYes of value 0 or 1.   
          In this way, I can generate report of sum(jtNo) and sum(jtYes) side by side grouped by publish state, 
          resource or both.
          
    5. DanubeResolverAnalysis
       A task for Rovi to ensure the java-transform system is compatible with Pri-java-transform system by analyzing 
       resolver.log entries.
       a) It adds "difference" display field by using format_string 
       b) It adds "diff. flag" field by using nested when and otheriwise sql functions on numeric conditions
       
    6. SparkSessionZipsExample to be familiar with Spark 2 Dataset/ DataFrame operation.
    
    7. FlghtSample is inspired by https://aws.amazon.com/blogs/aws/new-apache-spark-on-amazon-emr/.  I used small 
       sample to get the taste of schema & data and validate before move to I do a full-fledge run in EMR. 
    
#### Notes regrading to Spark integrated with Hadoop
    Cloudera latest CDH 5.11.x only works with Spark 1.6.0.  Cloudera Distribution of Apache Spark 2 only works with
    Cloudera Manager and is distributed as two files: a CSD file and a parcel. Follow the instruction on 
    https://www.cloudera.com/documentation/spark2/latest/topics/spark2_installing.html to install them.
       
    To get Apache Spark 2.x work with Apache Hadoop in pseudo-distributed mode, we need to install compatible version of
    hadoop (spark-2.1.1-bin-hadoop2.7 only works with hadoop 2.7). Follow the instruction 
    http://hadoop.apache.org/docs/r2.7.3/hadoop-project-dist/hadoop-common/SingleCluster.html    
    
        1. I previously already set JAVA_HOME and verified. start-dfs.sh still did not render Java path correctly. 
           I have to set JAVA_HOME in hadoop-env.sh.  To prepare for integration, I also set HADOOP_HOME, 
           HADOOP_CONF_DIR, SPARK_HOME and add $HADOOP_HOME/bin to path so I can use hadoop, hdfs, yarn command. 
        2.
            dfs.name.dir= ${hadoop.tmp.dir}/dfs/name      
           	dfs.data.dir=${hadoop.tmp.dir}/dfs/data
           	fs.checkpoint.dir=${hadoop.tmp.dir}/dfs/namesecondary         _
           	
           	are all controlled by hadoop.tmp.dir.  Hadoop use hadoop.tmp.dir as local tmp directory and also in HDFS.
           	The default value is /tmp/hadoop-${user.name}.  HDFS format would be gone after the box was rebooted.
           	Suggest to override it in the core-site.xml.
           	
        3. It's not enough to just specify --master yarn to signal spark to run in yarn. 
           Need to set (export) HADDOP_CONF_DIR too, $HADOOP_HOME/etc/hadoop in this case.
             	
        4. I failed to run --master yarn in the beginning and got error something 
           like "Container .. is running beyond virtual memory limits..." There is a check placed at Yarn level for 
           virtual and physical memory usage ratio. Issue is not only that VM doesn't have sufficient physical memory. 
           But it is because virtual memory usage is more than expected for given physical memory.  Fix by adding             
               <property>
                  <name>yarn.nodemanager.vmem-check-enabled</name>
                   <value>false</value>
                   <description>Whether virtual memory limits will be enforced for containers</description>
                 </property>
                <property>
                  <name>yarn.nodemanager.vmem-pmem-ratio</name>
                   <value>4</value>
                   <description>Ratio between virtual memory to physical memory when setting memory limits for containers</description>
                 </property>
           
           to yarn-site.xml.
               	
        5. The default of spark.sql.shuffle.partitions is 200 which is too high in test environment.   I set it to
           num-executors(2) * executor-cores(4) = 8.  My VM has 2 processors and 6 cores per processor.  I added
           spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048' to avoid stackOverflow too.
           Other common tuned up parameters are driver-memory and executor-memory.  Another option is to set
           spark.dynamicAllocation.enabled to true.  Running executors with too much memory often results in excessive 
           garbage collection delays. 64GB is a rough guess at a good upper limit for a single executor.
           HDFS client has trouble with tons of concurrent threads. A rough guess is that at most five tasks 
           per executor can achieve full write throughput. 
           
           See http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/ for the guideline
              	
        6. There is difference between yarn client and cluster deploy-mode. (the default is client if not specify.
           Set --deploy-mode cluster to override).  In client deploy-mode, the driver will be running on the machine 
           you started the job.  In cluster mode, the driver is running as a thread of the yarn application master
           If there is big network overhead between the machine that the job started and the cluster 
           (ex, your laptop), use cluster mode. if you submit application from a gateway machine that is physically
           co-located with your worker machines, client mode is appropriate. In client mode, 
           the input and output of the application is attached to the console.
           
           In yarn-cluster mode, the application master runs the driver, so it’s often useful to bolster its resources
           with the --driver-memory and --driver-cores properties.
           
        7. Each NodeManager would have its processing logged to the location defined in 
           yarn.nodemanager.remote-app-log-dir. To view it aggregately, I have to have
            <property>
                   <name>yarn.log-aggregation-enable</name>
                   <value>true</value>
               </property>

           in yarn-site.xml.  In this way, I am able to use "yarn logs -applicationId <app ID>" to get 
           consolidated application logs from all nodes.  
                	
        8. To be able to access application logs even after the application is done, we need to configure hadoop
           job history server and add 
             <property>
                 <name>mapreduce.jobhistory.address</name>
                 <value>ubuntu:10020</value>
               </property>
               <property>
                 <name>mapreduce.jobhistory.webapp.address</name>
                 <value>ubuntu:19888</value>
               </property>
            
           to mapred-site.xml and start job-history server.
            
           Also add 
              <property>
                 <name>yarn.log.server.url</name>
                 <value>http://ubuntu:19888/jobhistory/logs</value>
               </property>
           to yarn-site.xml so that resource-manager web site can re-direct to job-history web site when we click the
           application link after the application finish running.
            
        9. The above is application log which is general to any Hadoop application.  There is event log which records 
           Spark job/stage/task, storage, executor metrics and job DAG (Directed acyclic graph) which is specific to 
           Spark.  
           We have to configure Spark history-server to specify the location of event log files by adding ex.
             SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory='hdfs://ubuntu:9000/var/log/spark'"
           to spark-env.sh. Then start Spark history-server.
             
           On the other side, applications submited must add
             --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://ubuntu:9000/var/log/spark
           to enable recording event and log them to the location expected by Spark history server
             
           There will be stdout and stderr links for each executor under Executor tab of Spark application in 
           Spark history server site (default to port 18080).  In the case of deploy-mode: cluster,  there would be
           stdout and stderr on the driver row since stdout and stderr will be in cluster (contrast to console in the 
           case of client deploy-mode).   Those links would be re-directed to Hadoop job-history server to retrieve
           the application log.   
             
        10. Normally, Spark would even out workloads among executors.   When Spark run in yarn-cluster and it would 
            use one core of one executor to run the driver tasks.  Therefore, you might see one executor having 
            more tasks than others.  In the case of MovieLensALS, job ran in cluster mode performs better than 
            the job ran in client mode.  It is possibly due to MovieLensALS having a lot of println and show .  
            In the case of client mode, they have to output to console. 
              
        11. Spark jobs are broken by actions like count, show and collect.  Ex, the job 0 and 1 are
              println(s"Rating Snapshot= ${mrDS.count}, ${prDS.count}")
             
            and Spark stages might be broken down by transformations requiring shuffle, like map or flatMap   
            val mrDS = spark.read.textFile(mrFile).map(parseRating).cache()
            the map transformation in the job 0 is an example.  The stage prior to the boundary will do 
            shuffle write and the stage following the boundary will do shuffle read.
             
            and tasks are brokn down by number of partitions.
               
        12. After analyzing DAG,
            I know roughly the corresponding line of codes with Spark job.  The best way to cut down time is to 
            reduce loops of param grid and reduce max_iter of ALS param.  Unfortunately, that would reduce the
            quality of recommendation.  I improve performance a little by broadcasting movieDS and baselineRmse.
              
        13.  Spark would create a zip with all jar files under $SPARK_HOME/jars folder and upload to each node in the
             cluster if neither spark.yarn.jars nor spark.yarn.archive is set.  Therefore, I created a spark-jars.zip
             and put in hdfs /var/lib/hadoop-fandev folder.
             
        14.  Here is my command 
             $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --executor-cores 4 \ 
             --conf spark.sql.shuffle.partitions=8 --conf spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048' \
             --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://ubuntu:9000/var/log/spark \
             --conf spark.yarn.archive=hdfs://ubuntu:9000/var/lib/hadoop-fandev/spark-jars.zip \
             --class org.freemind.spark.sql.MovieLensALS target/scala-2.11/spark_tutorial_2_2.11-1.0.jar \ 
             input_movielen/ratings.dat.gz input_movielen/personalRatings.txt input_movielen/movies.dat  
        
        
                              
                              
       
              
             

             
               
            
             