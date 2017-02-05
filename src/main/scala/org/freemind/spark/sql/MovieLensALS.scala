package org.freemind.spark.sql

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Inspired by http://cdn2.hubspot.net/hubfs/438089/notebooks/MongoDB_guest_blog/Using_MongoDB_Connector_for_Spark.html
  *
  *  However, that notebook generate all prediction 0.0. I did myself and find the root cause and completely revise it.
  *  This is related to Jira ticket SPARK-14489
  *  https://www.mail-archive.com/issues@spark.apache.org/msg122962.html
  *  http://stackissue.com/apache/spark/spark-14489mlpyspark-als-unknown-user-item-prediction-strategy-12896.html
  *
  *  To the point, when the test/ validation data has user or item outside of training set,
  *  ALS model.transform will generate entries with prediction of NaN.  That cause any evaluation metrics,
  *  including rmse, returning NaN. which make recommendation having all predictions 0.0
  *  I tried ml-1m data and use old-fashion way - loop to find the best model based upon the least rmse.
  *  I got bestModel None.because of NaN rmse regardless of regParm and rank value.
  *  I took the advice from my research and get valid best model by excluding NaN from predication
  *
  *  The Notebook use TrainValidationSplit which did all work automatically and
  *  leave no way to manually remove all NaN prediction
  *
  *  I instilled concepts learned from rom edx.org BerkeleyX course "CS120x: Distrubuted Machine Learning with Apache Spark"
  *
  *  recommend.log use unratedDF generated with allDS distinct movieId then join movie (because I have to get name of movies)
  *  recommend2.log use unratedDF generated with movieDS directly. This is more efficient by saving distinct
  *  and join operation. However, I have to filter out NaN in this cases because there are some movies that have not been rated.
  *
  *  There is one remaining issue.  There are accented characters in title of movies like La Vita è bella.
  *  They display correctly after spark streaming: spark.read.textFile(movieFile).map(parseMovie).  However,
  *  those accented characters are lost in further Spark DataFrame operation like recommendation transformation.
  *  La Vita è bella become La Vita � bella (see recommend.log, recommend2.log or reccommend_mong.log
  *
  *  My wild guess is that a Dataset is a strongly typed collection of domain-specific objects
  *  To efficiently support domain-specific objects, an Encoder is required. The encoder maps the high-level
  *  domain specific type T to Spark's internal type system which is a low-level binary structure.
  *  Need to work on org.apache.spark.sql.Encoder
  *
  *  I first explores this issue on October 2016.
  * @author sling(threecuptea) re-wrote on 12/29/16 - 02/04/17
  */

case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

object MovieLensALS {

  def parseRating(line: String): Rating = {
    val splits = line.split("::")
    assert(splits.length == 4)
    Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
  }

  def parseMovie(line: String): Movie = {
    val splits = line.split("::")
    assert(splits.length == 3)
    Movie(splits(0).toInt, splits(1), splits(2).split('|'))
  }

  def main(args: Array[String]): Unit = {
    //add user and movie later
    if (args.length != 3) {
      println("Usage: MovieLensALS [movie_ratings] [ersonal_ratings] [movies]")
      System.exit(-1)
    }
    val mrFile = args(0)
    val prFile = args(1)
    val movieFile = args(2)

    val spark = SparkSession
      .builder()
      .appName("MovieLensALS")
      .getOrCreate()
    import spark.implicits._

    val mrDS = spark.read.textFile(mrFile).map(parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(parseRating).cache()
    val movieDS = spark.sparkContext.broadcast(spark.read.textFile(movieFile).map(parseMovie).cache())
    println(s"Rating Snapshot= ${mrDS.count}, ${prDS.count}")
    mrDS.show(10, false)
    println(s"Movies Snapshot= ${movieDS.value.count}")
    movieDS.value.show(10, false)

    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8,0.1,0.1)) //Follow the instruction of EDX class, use the model evaluated based upon validationSet on test
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
    // It penalize Linear Regression Model with large coefficients.
    // Linear Regression Model with large coefficients tends to be more complicated.
    //The default is 1.0
    val regParams = Array(0.1, 0.01)

    val als = new ALS().setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

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
      //Filter has different flavors.
      //def filter(func: (T) ⇒ Boolean): Dataset[T] for the followings
      //val prediction = model.transform(valDS).filter(r => !r.getAs[Float]("prediction").isNaN) //Need to exclude NaN from prediction, otherwise rmse will be NaN too
      //def filter(condition: Column): Dataset[T], for the following
      val prediction = model.transform(valDS).filter(!$"prediction".isNaN)
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
        val testPrediction = goodModel.transform(testDS).filter(!$"prediction".isNaN)
        val testRmse = evaluator.evaluate(testPrediction)
        val improvement = (baselineRmse - testRmse) / baselineRmse * 100
        println(s"The best model was trained with param = ${bestParam.get}")
        println()
        printf("The RMSE of the bestModel on test set is %3.2f, which is %3.2f%% over baseline.\n", testRmse, improvement) //use %% to print %
    }

    //Recall bestModel was fit with trainPlusDS, which cover userId=0 but might miss some movie
    //Need to study machine learning course
    val augmentModel = als.fit(allDS, bestParam.get)

    val pMovieIds = prDS.map(_.movieId).collect() // This should not be big, all movies that the person has rated
    val pUserId = 0

    //We can use allDS distinct movieId then join movieDS, We awill save join step by using movieDS directly. However,
    //we have to filter out NaN because Movies might have movieId not in allDS (Some movies have not been rated before)
    val unratedDF = movieDS.value.filter(movie => !pMovieIds.contains(movie.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId))

    //movieDS has more movies than allDS.  Therefore, we will getNaN.
    val recommendation = augmentModel.transform(unratedDF).filter(!$"prediction".isNaN).sort(desc("prediction"))
    recommendation.show(50, false)

    printf("Execution time= %7.3f seconds\n", (System.currentTimeMillis() - start)/1000.00)
  }

}


