package org.freemind.spark.sql

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


/**
  *  ALS model.transform will generate entries with prediction of NaN.  That cause any evaluation metrics,
  *  including rmse, returning NaN. which make final recommendation having all predictions 0.0
  *
  *  Spark starts to add coldStartStrategy to ALS at version 2.2.  It will drop those entries with NaN if I specify
  *  coldStartStrategy = "drop".  Therefore, I don't need manually filter(!$"prediction".isNaN)
  *
  *  In this version,  I tried
  *  the combination of
  *  1. Use ALS (coldStartStrateg= "drop") directly to fit and transform
  *
  *  1. Recommendation comes from manual creating unratedMovie set for userId = X then transform on it
  *     The advantage is small set of data (only for one user).  It's only for one user.  However, I have to repeat the
  *     same for others.
  *  2. Recommendation comes from {@link ALSModel#recommendForAllUsers} which
  *     return Rows of (userId: int, recommendations: array&lt;struct&lt;movieId:int,rating:float&gt;&gt;])
  *     then explode recommendations field to flatten it then retrieve individual fields of type struct.
  *     The advantage of this method is that the result can be cached and be used for multiple users.
  *
  * @author sling/ threecuptea on 2/4/2018, refined on 2/17/2018
  *
  */
object MovieLensALSColdStart {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MovieLensALSColdStart [movie_ratings] [personal_ratings] [movies]")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("MovieLensALSColdStart").config("spark.sql.shuffle.partitions", 8).getOrCreate()
    import spark.implicits._

    val mrFile = args(0)
    val prFile = args(1)
    val movieFile = args(2)

    val mlParser = new MovieLensParser()
    val mrDS = spark.read.textFile(mrFile).map(mlParser.parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(mlParser.parseRating).cache()
    val movieDS = spark.read.textFile(movieFile).map(mlParser.parseMovie).cache()

    mrDS.show(10, false)
    println(s"Rating Counts: movie - ${mrDS.count}, personal - ${prDS.count}")
    movieDS.show(10, false)
    println(s"Movie Count: ${movieDS.count}")
    //Initialize data for augmented model
    val allDS = mrDS.union(prDS).cache()

    //Initialize evaluator
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

    /**
      * ALS codes starts here
      * ALS (alternating least square is a popular model that spark-ml use for 'Collaborative filtering'
      * KEY POINT is coldStartStrategy = "drop"
      */
    val als = new ALS()
      .setMaxIter(20).setColdStartStrategy("drop").setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    val ranks = Array(10, 12)  //numbers of latent factor used to predict missing entries of user-item matrics, the default is 10
    val regParams = Array(0.8, 0.5, 0.1) //the default is 1.0
    //Array[ParamMap]
    val paramGrids = new ParamGridBuilder()
      .addGrid(als.regParam, regParams)
      .addGrid(als.rank, ranks)
      .build()

    val bestParmsFromALS = getBestParmMapFromALS(als, paramGrids, evaluator, mrDS, prDS)
    //I I can get bestParams because I manually go through paramGrids to calculate and find the best params
    //I cannot do that for CrossValidator
    println(s"The best model from ALS was trained with param = ${bestParmsFromALS}")

    val augModelFromALS = als.fit(allDS, bestParmsFromALS)  //Refit.  It is more accurate

    //manual way, it is much light-weighted for one user if you have so many users
    val pMovieIds = prDS.map(_.movieId).collect()
    val pUserId = 0
    //So that I can match ALS column name and also have movie title
    val pUnratedDS = movieDS.filter(mv => !pMovieIds.contains(mv.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId))
    //Using ALS manual
    println(s"The recommendation on unratedMovie for user ${pUserId} from ALS model")
    augModelFromALS.transform(pUnratedDS).sort(desc("prediction")).show(10, false)

    //recommendation: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [userId: int, recommendations: array<struct<movieId:int,rating:float>>]
    //We explode to flat array then retrieve field from a struct
    val recommendDS = augModelFromALS.recommendForAllUsers(10).
      select($"userId", explode($"recommendations").as("struct")).
      select($"userId", $"struct".getField("movieId").as("movieId"), $"struct".getField("rating").as("rating")).cache()

    println(s"The top recommendation on AllUsers filter with  user ${pUserId} from ALS model")
    recommendDS.filter($"userId" === pUserId).
      join(movieDS, recommendDS("movieId") === movieDS("id")).select($"movieId", $"title", $"genres", $"userId", $"rating").show(false)

    val sUserId = 6001
    val sMovieIds = mrDS.filter($"userId" === sUserId).map(_.movieId).collect();
    val sUnratedDS = movieDS.filter(mv => !sMovieIds.contains(mv.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(sUserId))
    println($"The recommendation on unratedMovie for user ${sUserId} from ALS model")
    augModelFromALS.transform(sUnratedDS).sort(desc("prediction")).show(10, false)

    println(s"The top recommendation on AllUsers filter with  user ${sUserId} from ALS model")
    recommendDS.filter($"userId" === sUserId).
      join(movieDS, recommendDS("movieId") === movieDS("id")).select($"movieId", $"title", $"genres", $"userId", $"rating").show(false)

  }

  def getBestParmMapFromALS(als: ALS, paramGrids: Array[ParamMap], evaluator: RegressionEvaluator,
                      mrDS: Dataset[Rating], prDS: Dataset[Rating]): ParamMap = {
    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8, 0.1, 0.1))
    trainDS.cache()
    valDS.cache()
    testDS.cache()
    val trainPlusDS = trainDS.union(prDS).cache()

    var bestModel: Option[ALSModel] = None
    var bestRmse = Double.MaxValue
    var bestParam: Option[ParamMap] = None

    for (paramMap <- paramGrids) {
      val model = als.fit(trainPlusDS, paramMap)
      val prediction = model.transform(valDS)
      val rmse = evaluator.evaluate(prediction)
      if (rmse < bestRmse) {
        bestRmse = rmse
        bestModel = Some(model)
        bestParam = Some(paramMap)
      }
    }

    bestModel match {
      case None =>
        println("Unable to find a valid ALSModel.  STOP here")
        throw new IllegalArgumentException("Unable to find a valid ALSModel")
      case Some(model) =>
        val prediction = model.transform(testDS)
        val rmse = evaluator.evaluate(prediction)
        //xtractParamMap only has default without additional
        //println(s"The best model from ALS was trained with param = ${model.extractParamMap}")
        printf("The RMSE of the bestModel from ALS on validation set is %3.4f\n", bestRmse)
        printf("The RMSE of the bestModel from ALS on test set is %3.4f\n", rmse)
        println()
        bestParam.get
    }
  }

}
