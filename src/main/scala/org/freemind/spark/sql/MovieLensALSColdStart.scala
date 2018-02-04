package org.freemind.spark.sql

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class Rating(userId: Int, movieId: Int, rating: Float)
case class Movie(id: Int, title: String, genres: Array[String])

object MovieLensALSColdStart {

  def parseRating(line: String): Rating = {
    val splits = line.split("::")
    assert( splits.length == 4)
    Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
  }

  def parseMovie(line: String): Movie = {
    val splits = line.split("::")
    assert( splits.length == 3)
    Movie(splits(0).toInt, splits(1), splits(2).split("|"))
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MovieLensALSColdStart [movie_ratings] [personal_ratings] [movies]")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("MovieLensALSColdStart").getOrCreate()
    import spark.implicits._

    val mrFile = args(0)
    val prFile = args(1)
    val movieFile = args(2)

    val mrDS = spark.read.textFile(mrFile).map(parseRating).cache()
    val prDS = spark.read.textFile(prFile).map(parseRating).cache()
    val movieDS = spark.read.textFile(movieFile).map(parseMovie).cache()

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

    val bestModelFromALS = getBestALSModel(als, paramGrids, evaluator, mrDS, prDS)
    val augModelFromALS = als.fit(allDS, bestModelFromALS.extractParamMap())  //Refit.  Is it necessary

    val cv = new CrossValidator()
      .setEstimator(als).setEvaluator(evaluator).setEstimatorParamMaps(paramGrids).setNumFolds(10)  // percentage close to 0.8, 0.1, 0.1
    val bestModelFromCv = getBestCrossValidatorModel(cv, paramGrids, evaluator, mrDS, prDS)
    val augModelFromCV = cv.fit(allDS, bestModelFromCv.extractParamMap())

    //manual way
    val pMovieId = prDS.map(_.movieId).collect()
    val pUserId = 0
    //So that I can match ALS column name and also have movie title
    val unRatedDS = movieDS.filter(mv => !pMovieId.contains(mv.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId))
    //Using ALS manual
    println("The recommendation on unratedMovie for user 0 from ALS model")
    augModelFromALS.transform(unRatedDS).sort(desc("prediction")).show(25, false)
    println("The recommendation on unratedMovie for user 0 from CrossValidator with ALS as estimator")
    augModelFromCV.transform(unRatedDS).sort(desc("prediction")).show(25, false)

    println("The top recommendation on AllUsers filter with  user 0 from ALS model")
    augModelFromALS.recommendForAllUsers(25).filter($"userId" === 0).show(false)
    println("The top recommendation on AllUsers filter with  user 0 from  CrossValidator with ALS as estimator")
    augModelFromCV.bestModel.asInstanceOf[ALSModel].recommendForAllUsers(25).filter($"userId" === 0).show(false)

  }

  def getBestALSModel(als: ALS, paramGrids: Array[ParamMap], evaluator: RegressionEvaluator,
                      mrDS: Dataset[Rating], prDS: Dataset[Rating]): ALSModel = {
    val Array(trainDS, valDS, testDS) = mrDS.randomSplit(Array(0.8, 0.1, 0.1))
    trainDS.cache()
    valDS.cache()
    testDS.cache()
    val trainPlusDS = trainDS.union(prDS).cache()

    var bestModel: Option[ALSModel] = None
    var bestRmse = Double.MaxValue

    val models: Seq[ALSModel] = als.fit(trainPlusDS, paramGrids)
    for (model <- models) {
      val prediction = model.transform(valDS)
      val rmse = evaluator.evaluate(prediction)
      if (rmse < bestRmse) {
        bestRmse = rmse
        bestModel = Some(model)
      }
    }

    bestModel match {
      case None =>
        println("Unable to find a valid ALSModel.  STOP here")
        throw new IllegalArgumentException("Unable to find a valid ALSModel")
      case Some(model) =>
        val prediction = model.transform(testDS)
        val rmse = evaluator.evaluate(prediction)
        println(s"The best model from ALS was trained with param = ${model.extractParamMap}")
        printf("The RMSE of the bestModel from ALS on validation set is %3.2f\n", bestRmse)
        printf("The RMSE of the bestModel from ALS on test set is %3.2f\n", rmse)
        println()
        model
    }
  }

  def getBestCrossValidatorModel(cv: CrossValidator, paramGrids: Array[ParamMap], evaluator: RegressionEvaluator,
                      mrDS: Dataset[Rating], prDS: Dataset[Rating]): CrossValidatorModel = {

    val Array(cvTrainValDS, cvTestDS) = mrDS.randomSplit(Array(0.9, 0.1))
    val cvTrainValPlusDS = cvTrainValDS.union(prDS).cache()

    val cvModel = cv.fit(cvTrainValPlusDS)
    val bestRmse = evaluator.evaluate(cvModel.transform(cvTrainValPlusDS))
    val cvPrediction = cvModel.transform(cvTestDS)
    val cvRmse = evaluator.evaluate(cvPrediction)
    println(s"The best model from CrossValidator was trained with param = ${cvModel.extractParamMap}")
    printf("The RMSE of the bestModel from CrossValidator on validation set is %3.2f\n", bestRmse)
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.2f\n", cvRmse)
    println()
    cvModel
  }
}
