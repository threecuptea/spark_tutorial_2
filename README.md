### spark-tutorial_2 collects small projects that I worked on using spark scala 2 Dataset/ DataFrame.  Those were first inpired by open source articles from Databricks community, LinkedIn 'Apache Spark' User Group.  I instilled my idea while rewriting. 
#### The topics include:
    1. A complete movie recommendation System using Spark-ML ALS (Alternating Least Square) algorithm -MovieLensALS
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
 
    2. Similar movie recommendation system but the source is data in MongoDB - MovieLensALSMongo
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
    
    8. MovieLensALSColdStart
       I mentioned the pitfall of NaN when items used to transform are outside of items used to fit.  Spark starts to 
       add coldStartStrategy to ALS since 2.2.  It will drop those entries with NaN if I specify 
       coldStartStrategy = "drop".  Therefore, I refined codes accordingly.   Also I explore 
       ALSModel.recommendForAllUsers(numItems) that introduced in 2.2 too which return Rows of 
       (userId: int, recommendations: array<struct<movieId:int,rating:float>>]).  That requires transformation to
       human reader form.  The result from manual unratedMovies and from recommendForAllUsers are very much the same
       except for one discrepancy.   Using recommendForAllUsers(numItems) pays the unfront cost for all users, 
       additional operation of filtering by userId and join with movies are minimal.  It is worthwhile if it is 
       required to provide recommendation for lots of users.
    
    9. MovieLensALSColdStartCv (CrossValidator)
       Apply coldStartStrategy = "drop" too.  However, I use CrossValidator (10 folds, one of 10 set in terms was 
       chosen as validation set and the rest as training set).  Let CrossValidator to find the best model.  The rmse on 
       validation set is very good 0.8111. However, the rmse on test set is not better off than ALS alone does 
       (0.8623 vs. 0.8567).  It seems to a little overfitting.        
       Getting the best paramaters from Spark CrossValidator is not straightforward as  Scikit-Learn's GridSearch
       The best option is to zip getEstimatorParamMaps and avgMetrics of the best CrossValidatorModel to get 
       the top one as the followings:
             
           val descArr = (bestModelFromCR.getEstimatorParamMaps zip bestModelFromCR.avgMetrics).sortBy(_._2)
           val bestParamMap = descArr(0)._1  
       
       I can refit 'bestParamMap' obtained the above as the followings:
           
            val augModelFromCv = als.fit(allDS, bestParamMap)  
                                                                                                  
       Use als instead of cv when refit the whole populationn with 'bestParamMap'
       I save the result of the CrossValidatorModel metadata in cv-model/metadata.  I verify the bestParamMap obtained 
       above with estimatorParamMaps and avgMetrics of saved model.
       
    10. OverlaysProcessing split into ContentOverlayProcessing and StationOverlayProcessing (latest): 
       This parses Tivo's show content/ collection specification CSV file
       a) To categorize resource_type using SHOWTYPE etc. criteria
       b) Call java static method (IdGen class) & get sbt java/ scala build working and put together a udf type to
          generate overlay numeric value for content/ collection/ station ids.
       c) Save key values to Mongodb  OverlaysLookup collection of unified database so that I can create 
          IdAuthResponse overlay simulator.  
            
       
#### Notes regrading to Spark integrated with Hadoop
    Cloudera latest CDH 5.14.x only works with Spark 1.6.0.  Cloudera Distribution of Apache Spark 2 only works with
    Cloudera Manager and is distributed as two files: a CSD file and a parcel. Follow the instruction on 
    https://www.cloudera.com/documentation/spark2/latest/topics/spark2_installing.html to install them.
       
    To get Apache Spark 2.x work with Apache Hadoop in pseudo-distributed mode, we need to install compatible version of
    hadoop (spark-2.2.1-bin-hadoop2.7 only works with hadoop 2.7). Follow the instruction 
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
           virtual and physical memory usage ratio. Issue is not that VM doesn't have sufficient physical memory 
           but it is because virtual memory usage is more than expected for given physical memory.  Fix by adding             
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
           num-executors(2) * executor-cores(4) = 8.  I added spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048' 
           to avoid stackOverflow too. Other common tuned up parameters are driver-memory and executor-memory.  
           Another option is to set spark.dynamicAllocation.enabled to true.  
           Running executors with too much memory often results in excessive garbage collection delays. 
           64GB is a rough guess at a good upper limit for a single executor. HDFS client has trouble with tons of 
           concurrent threads. A rough guess is that at most five tasks per executor can achieve full write throughput. 
           Running tiny executors with with a single core throws away the benefits running multiple tasks in a single 
           JVM.
           
           Node manager hierarchy is as the followings:
           'yarn.nodemanager.resource.memory-mb' can have multiple executor container and each executor container 
           have two parts: 'executor.memory' and 'spark.yarn.executor.memoryOverhead' which is the maximum of (384 or 
           0.07 * executor memory).
           
           One node can host multiple executors ( no one-to-one requirement) and one executor can have multiple 
           executor-core.   
           
           A Yarn cluster with 6 nodes, each equipped with 16 cores and 64GB of memory is best configured with 
           --num-executor 17 --executor-core 5 --executor-memory 19GB instead of
           --num-executor 6 --executor-core 15 --executor-memory 63GB taking consideration of 
           'spark.yarn.executor.memoryOverhead' and AM (Appliocation Master must run in one core
           
           The hierarchy of Spark execution: job, stage and tasks. The boundary of job is an action and the boundary of
           stage is transformation like *ByKey and re-partition, join, cogroup etc. which are involved shuffled read-write
           The boundary of tasks are decided by number of partitions of RDD that the stage is working on. The very 
           root of data via textFile or hadoopFile are decided by inputFormat, ie HDFS block size (default = 64MB)
           groupByKey write whole objects into memory buffer and reduceByKey only write aggregated values into memory
           The former requires more memory.
                          
           See http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/ for the guideline
              	
        6. There is difference between yarn client and cluster deploy-mode. (the default is client if not specify.
           Set --deploy-mode cluster to override).  In client deploy-mode, the driver will be running on the machine 
           you run spark-submit.  In cluster mode, the driver is running on a core(as a thread) of the yarn 
           application master If there is big network overhead between the machine that the job started and the cluster 
           (ex, your laptop), use cluster mode. if you submit application from a gateway machine that is physically
           co-located with your worker machines, client mode is appropriate. In client mode, 
           the input and output of the application is attached to the console. In cluster mode, the output is in stadout.
           
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
           We have to start Spark history-server to specify the location it expects that applications write logs to like
           $SPARK_HOME/sbin/start-history-server.sh hdfs://ubuntu:9000/var/log/spark
             
           On the other side, applications submited must add
               --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://ubuntu:9000/var/log/spark
           to enable recording event and log them to the location expected by Spark history server
             
           There will be stdout and stderr links for each executor under Executor tab of Spark application in 
           Spark history server site (default to port 18080).  In the case of deploy-mode: cluster,  there would be
           stdout and stderr links on the driver row since stdout and stderr will be in cluster (contrast to console in the 
           case of client deploy-mode).   Those links would be re-directed to Hadoop job-history server to retrieve
           the application log.   
             
        10. Normally, Spark would even out workloads among executors.   When Spark run in yarn-cluster and it would 
            use one core to run the driver tasks.  Therefore, you might see one executor having 
            more tasks than others.  In the case of MovieLensALS, job ran in cluster mode performs better than 
            the job ran in client mode.  It is possibly due to MovieLensALS having a lot of println and show .  
            In the case of client mode, they have to output to console. 
              
        11. Spark jobs are broken by actions like count, show and collect.  Ex, the job 0 and 1 are
               println(s"Rating Snapshot= ${mrDS.count}, ${prDS.count}")
             
            and Spark stages might be broken down by transformations requiring shuffle, like map or flatMap
               
               val mrDS = spark.read.textFile(mrFile).map(parseRating).cache()
               
            the map transformation in the job 0 is an example.  The stage prior to the boundary will do 
            shuffle write and the stage following the boundary will do shuffle read.

               
        12. After analyzing DAG,
            I know roughly the corresponding line of codes with Spark job.  The best way to cut down time is to 
            reduce loops of param grid and reduce max_iter of ALS param.  Unfortunately, that would reduce the
            quality of recommendation.  I improve performance a little by broadcasting movieDS and baselineRmse.
              
        13.  Spark would create a zip with all jar files under $SPARK_HOME/jars folder and upload to each node in the
             cluster if neither spark.yarn.jars nor spark.yarn.archive is set.  Therefore, I created a spark-jars.zip
             and put in hdfs /var/lib/hadoop-fandev folder.
             
        14.  Here is my final command after a couple of tune ups
             $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --executor-cores 5 \ 
             --conf spark.sql.shuffle.partitions=8 --conf spark.executor.extraJavaOptions='-XX:ThreadStackSize=2048' \
             --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://ubuntu:9000/var/log/spark \
             --conf spark.yarn.archive=hdfs://ubuntu:9000/var/lib/hadoop-fandev/spark-jars.zip \
             --class org.freemind.spark.sql.MovieLensALS target/scala-2.11/spark_tutorial_2_2.11-1.0.jar \ 
             input_movielen/ratings.dat.gz input_movielen/personalRatings.txt input_movielen/movies.dat  
        
             I achieved total running duration 1 min and 46 sec. from the time that I started ALS modeling.  My VM was 
             8 processors (1 core per processor).  Spark default without any tuning are --num-executors 2 
             --executor-cores 1 --executor-memory 1g --driver-cores 1 --driver-memory 1g.  Here are some of observations:
             
             a. executor-cores affects most.  Performance almost double when I increased it from 3 (1.6 min) to 5 
                (1 min) for 20 ALS maxIter.  For some job/ stage works backed by RDD, including the majority of ALS 
                works, which decides number of partitions/ tasks based upon input format (block size), in this case 
                10 partitions, able to load balancing those tasks definitely improves the throughput. I took advice 
                from Sandy Ryza's article that "HDFS client has trouble with tons of concurrent threads. 
                A rough guess is that at most five tasks per executor can achieve full write throughput".
                
             b. num of paramMap decides no. loops of repeated ALS fit and RegressionEvaluator evaluate jobs.  Each loop 
                accounts for 6 jobs in MovieLensALS.  Some jobs are 100% correlated with maxIter param like 
                'count at ALS.scala:944', increasing no. of stages from 22 to 42 when I increase maxIter from 10 to 20.
                There is trade-off of performance vs. rmse in terms of value of maxIter.
                
             c. CrossValidator is very time consuming because the above time have to times No. of folds.  In this 
                case, it is 10.  Also it requires lots of memory.  I couldn't adjust --core-memory to 2g.  2 * 2g +
                1g (driver) will execeeds available memory.  
                
             d. It takes 7 -11 sec. to run recommendForAllUsers(numItems). Yes, we pays the unfront cost for all users.       
                
             e. I adjusted spark.sql.shuffle.partitions to 8 because the default is 200 which is way too high in test
                environment.  That only affects Spark-SQL operations and does not affect operations involved RDD 
                partitions.
                
             f. I also tried org.apache.spark.serializer.KryoSerializer which does not help in the psedu-distributed 
                environment (It might help in real network environment).  I also tried dynamicAllocation following 
                appropriate yarn shuffle instructions and configures with --conf spark.shuffle.service.enabled=true 
                --conf spark.dynamicAllocation.enabled=true.  It keeps using 
                --conf spark.dynamicAllocation.minExecutors=1 setting and did not bounce up # executors.  
                                    
                
            
                
                 
                
             
             
             
             
             
                                
                              
       
              
             

             
               
            
             