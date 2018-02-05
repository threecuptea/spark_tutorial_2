package org.freemind.spark.sql

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


/**
  *  ALS model.transform will generate entries with prediction of NaN.  That cause any evaluation metrics,
  *  including rmse, returning NaN. which make final recommendation having all predictions 0.0
  *
  *  Spark starts to add coldStartStrategy to ALS.  It will drop those entries with NaN if I specify
  *  coldStartStrategy = "drop".  Therefore, I don't need manually filter(!$"prediction".isNaN)
  *
  *  In this version,  I tried
  *  the combination of
  *  1. Use ALS (coldStartStrateg= "drop") directly to fit and transform
  *      or
  *  2. Use CrossValidator 10-fold with ALS (coldStartStrateg= "drop") as the estimator
  *   and
  *  1. Recommendation comes from manual creating unratedMovie set for userId = 0 then transform on it
  *  2. Recommendation comes from {@link ALSModel#recommendForAllUsers} filter userId === 0
  *
  * @author sling/ threecuptea on 2/4/2018
  *
  */
object MovieLensALSColdStart {

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
    println(s"The best model from ALS was trained with param = ${bestParmsFromALS}")
    val augModelFromALS = als.fit(allDS, bestParmsFromALS)  //Refit.  Is it necessary

    val cv = new CrossValidator()
      .setEstimator(als).setEvaluator(evaluator).setNumFolds(10)  // percentage close to 0.8, 0.1, 0.1
    val bestModelFromCv = getBestCrossValidatorModel(cv, paramGrids, evaluator, mrDS, prDS)
    val augModelFromCV = cv.fit(allDS, bestModelFromCv.extractParamMap())
    */
    //manual way
    val pMovieId = prDS.map(_.movieId).collect()
    val pUserId = 0
    //So that I can match ALS column name and also have movie title
    val unRatedDS = movieDS.filter(mv => !pMovieId.contains(mv.id)).withColumnRenamed("id", "movieId").withColumn("userId", lit(pUserId))
    //Using ALS manual
    println("The recommendation on unratedMovie for user 0 from ALS model")
    augModelFromALS.transform(unRatedDS).sort(desc("prediction")).show(25, false)
    /*
    println("The recommendation on unratedMovie for user 0 from CrossValidator with ALS as estimator")
    augModelFromCV.transform(unRatedDS).sort(desc("prediction")).show(25, false)

    println("The top recommendation on AllUsers filter with  user 0 from ALS model")
    augModelFromALS.recommendForAllUsers(25).filter($"userId" === 0).show(false)
    println("The top recommendation on AllUsers filter with  user 0 from  CrossValidator with ALS as estimator")
    augModelFromCV.bestModel.asInstanceOf[ALSModel].recommendForAllUsers(25).filter($"userId" === 0).show(false)
    */
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
