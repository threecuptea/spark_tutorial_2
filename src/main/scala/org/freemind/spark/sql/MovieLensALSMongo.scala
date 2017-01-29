package org.freemind.spark.sql

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



/**
  * http://cdn2.hubspot.net/hubfs/438089/notebooks/MongoDB_guest_blog/Using_MongoDB_Connector_for_Spark.html
  *
  * Using convert_csv.py to convert "::" delimiter to "," then
  * mongoimport -d movielens -c movie_ratings --type csv -f user_id,movie_id,rating,timestamp data/ratings.csv
  *
  * I discovered one serious issue here.  The recommendation is very different from the recommendation from MovieLensALS.
  * PersonalRatings comes from my choice.  the recommendation from MovieLensALS fit my taste much better than the recommendation here.
  *
  * It turns out Mongo Spark 2 connector introduce a new bug when doing randomSplit.  The sum up of all splits does not equal to
  * the count of whole (the toal: 1000209).  That distorts the results.  I opened a JIRA ticket on Mongo spark connector.
  *
  * Spark2 randomSplit is different from Spark 1.6.  Spark 1.6 always return the same result of the same count and thhe same seee.
  * even when file content is different.   Spark2 is different.  It really randomly split.  Checkout recommend.log and recommend2.log
  * which I generated using MovieLensALS in different run.
  * The splits are different.  However, the total always match the count of the whole.
  *
  * Try both spark 2.1.0 and spark 2.0.2 and get the same results.
  *
  * $SPARK_HOME/bin/spark-submit --jars jars/org.mongodb.spark_mongo-spark-connector_2.11-2.0.0.jar,jars/org.mongodb_mongo-java-driver-3.2.2.jar \
  * --master local[*] --class org.freemind.spark.sql.MovieLensALSMongo target/scala-2.11/spark_tutorial_2_2.11-1.0.jar
  *
  * @author sling(threecuptea) wrote on 12/30/16.
  */
//Most MongoDB collection and field name use _ convention
case class MongoRating(user_id: Int, movie_id: Int, rating: Float)
case class MongoMovie(id: Int, title: String, genres: Array[String])

object MovieLensALSMongo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("MovieLensALSMongo")
      .getOrCreate()
    import spark.implicits._

    val mrReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.movie_ratings?readPreference=primaryPreferred"))
    val prReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.personal_ratings?readPreference=primaryPreferred"))
    val movieReadConfig = ReadConfig(Map("uri" -> "mongodb://localhost:27017/movielens.movies?readPreference=primaryPreferred"))
    val recomWriteConfig = WriteConfig(Map("uri" -> "mongodb://localhost:27017/movielens.recommendations"))
    //MongoSpark.toDS route does not work
    //val mrDS = MongoSpark.load(spark, mrReadConfig, classOf[MongoRating]) //Will get error 'Cannot infer type for class org.freemind.spark.sql.MongoRating because it is not bean-compliant'
    //I have to use old route toDF then as to DS.  MongoSpark.load(spark, mrReadConfig) return DataFrame then map to type
    val mrDS = MongoSpark.load(spark, mrReadConfig).map(r => MongoRating(r.getAs[Int]("user_id"), r.getAs[Int]("movie_id"), r.getAs[Int]("rating")))
    val prDS = MongoSpark.load(spark, prReadConfig).map(r => MongoRating(r.getAs[Int]("user_id"), r.getAs[Int]("movie_id"), r.getAs[Int]("rating")))
    val movieDS = MongoSpark.load(spark, movieReadConfig).map(r => MongoMovie(r.getAs[Int]("id"), r.getAs[String]("title"), r.getAs[String]("genre_concat").split("\\|")))
    val bMovieDS = spark.sparkContext.broadcast(movieDS)

    println(s"Rating Snapshot= ${mrDS.count}, ${prDS.count}")
    mrDS.show(10, false)
    println(s"Movies Snapshot= ${bMovieDS.value.count}")
    bMovieDS.value.show(10, false)

    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8,0.1,0.1)) //Follow the instruction of EDX class, use the model getting from validationSet on test
    println(s"training count=${trainDS.count}")
    println(s"validation count=${valDS.count}")
    println(s"test count=${testDS.count}")
    val total = trainDS.count() + valDS.count() + testDS.count()
    println(s"TOTAL COUNT=$total")
    println()


    valDS.cache()
    testDS.cache()
    val trainPlusDS = trainDS.union(prDS).cache() //used to fit
    val allDS = mrDS.union(prDS).cache()

    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val avgRating = trainDS.select(mean($"rating")).first().getDouble(0) //directly select mean($"rating") => DF => first() row, get the first field
    val baselineRmse = evaluator.evaluate(testDS.withColumn("prediction", lit(avgRating)))
    printf("The baseline rmse= %3.2f.\n", baselineRmse)
    println()

    /**
      * ALS codes starts here
      * ALS (alternating least square is a popular model that spark-ml use for 'Collaborative filtering'
      */
    //I tried more than those to narrow down the followings,
    val start = System.currentTimeMillis()
    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val iters = Array(20) //the default is 10
    //It is a lambda multipler on the cost to prevent overfitting.  Increasing lambda will penalize the cost which are coefficients of linear regression
    //The default is 1.0
    val regParams = Array(0.1, 0.01)

    val als = new ALS().setUserCol("user_id").setItemCol("movie_id").setRatingCol("rating")

    val paramGrids = new ParamGridBuilder()
      .addGrid(als.rank, ranks)
      .addGrid(als.maxIter, iters)
      .addGrid(als.regParam, regParams)
      .build() //build return Array[ParamMap]

    var bestRmse = Double.MaxValue
    var bestModel: Option[ALSModel] = None
    var bestParam: Option[ParamMap] = None

    for (paramMap <- paramGrids) {
      val model = als.fit(trainPlusDS, paramMap)
      //transform returns a DF with additional field 'prediction'
      val prediction = model.transform(valDS).filter(r => !r.getAs[Float]("prediction").isNaN) //Need to exclude NaN from prediction, otherwise rmse will be NaN too
      val rmse = evaluator.evaluate(prediction)
      //NaN is bigger than maximum
      if (rmse < bestRmse)       {
        bestRmse = rmse
        bestModel = Some(model)
        bestParam = Some(paramMap)
      }
    }


    bestModel match {
      case None =>
        println("Unable to find a good ALSModel.  STOP here")
        System.exit(-1)
      case Some(goodModel) =>
        //We still need to filter out NaN
        val testPrediction = goodModel.transform(testDS).filter(r => !r.getAs[Float]("prediction").isNaN)
        val testRmse = evaluator.evaluate(testPrediction)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        println(s"The best model was trained with param = ${bestParam.get}")
        println()
        printf("The RMSE of the bestModel on test set is %3.2f, which is %3.2f%% over baseline.\n", testRmse, improvement) //use %% to print %
    }

    //Recall bestModel was fit with trainPlusDS, which cover userId=0 but might miss some movie
    //Need to study machine learning course
    val augmentModel = als.fit(allDS, bestParam.get)
    //map, filter are typed operation on Dataset is checked
    val pMovieIds = prDS.map(_.movie_id).collect() // This should not be big, all movies that the person has rated
    val pUserId = 0

    //We can use allDS distinct movieId then join movieDS, We awill save join step by using movieDS directly. However,
    //we have to filter out NaN because Movies might have movieId not in allDS (Some movies have not been rated before)
    val unratedDF = bMovieDS.value.filter(movie => !pMovieIds.contains(movie.id)).withColumnRenamed("id", "movie_id").withColumn("user_id", lit(pUserId)) //als use those fields

    //com.mongodb.spark.exceptions.MongoTypeConversionException: Cannot cast 4.591038 into a BsonValue. FloatType has no matching BsonValue.  Try DoubleType
    val recommendation = augmentModel.transform(unratedDF).filter(r => !r.getAs[Float]("prediction").isNaN).sort(desc("prediction"))
    recommendation.show(50, false)

    //MongoSpark.save(recommendation.select($"movie_Id", $"title", $"prediction".cast(DoubleType)).write.mode("overwrite"), recomWriteConfig)


    printf("Execution time= %7.3f seconds\n", (System.currentTimeMillis() - start)/1000.00)
  }


}
