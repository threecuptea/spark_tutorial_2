package org.freemind.spark.sql

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}


/**
  *  ALS model.transform will generate entries with prediction of NaN.  That cause any evaluation metrics,
  *  including rmse, returning NaN. which make final recommendation having all predictions 0.0
  *
  *  Spark starts to add coldStartStrategy to ALS from version 2.2 up.  It will drop those entries with NaN if I specify
  *  coldStartStrategy = "drop".  Therefore, I don't need manually filter(!$"prediction".isNaN)
  *
  *  In this version,  I tried
  *  the combination of
  *
  *  2. Use CrossValidator 10-fold with ALS (coldStartStrateg= "drop") as the estimator
  *   and
  *  1. Recommendation comes from manual creating unratedMovie set for userId = 0 then transform on it
  *
  *  save CrossValidatorModel and reload it back
  *
  *  To separate CrossValidator from ALS stanalone because CrossValidator take too much time  to run.
  *  It's difficult for me to refine.
  *
  * @author sling/ threecuptea on 2/4/2018, refined on 2/17/2018 and 3/3/2018
  *
  */
object MovieLensALSColdStartCv {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: MovieLensALSColdStartCv [movie_ratings] [personal_ratings] [movies]")
      System.exit(-1)
    }

    val spark = SparkSession.builder().appName("MovieLensALSColdStartCv").getOrCreate()
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

    val cv = new CrossValidator()
        .setEstimator(als).setEvaluator(evaluator).setEstimatorParamMaps(paramGrids).setNumFolds(10)  // percentage close to 0.8, 0.1, 0.1
    //Why does this display CvModel instead ALSModel parameters.  That's different from what I got from Housing spark
    val bestModelFromCR = getBestCrossValidatorModel(cv, mrDS, prDS)
    println(bestModelFromCR.bestModel.explainParams()+"\n")
    //The following is one way to get best param
    val descArr = (bestModelFromCR.getEstimatorParamMaps zip bestModelFromCR.avgMetrics).sortBy(_._2)
    val bestParamMap = descArr(0)._1
    println(s"The best ALS model obtained from CVModel was trained with param = ${bestParamMap}")
    val augModelFromCv = als.fit(allDS, bestParamMap) //Refit. No need to use CV and CV is a mean to get bestParam of the estimator

    val recommendDS = augModelFromCv.recommendForAllUsers(10).
      select($"userId", explode($"recommendations").as("recommend")).
      select($"userId", $"recommend".getField("movieId").as("movieId"), $"recommend".getField("rating").as("rating")).cache()

    val pUserId = 0
    println(s"The top recommendation on AllUsers filter with  user ${pUserId} from ALS model from CV")
    recommendDS.filter($"userId" === pUserId).
      join(movieDS, recommendDS("movieId") === movieDS("id")).
      select($"movieId", $"title", $"genres", $"userId", $"rating").show(false)

    val identifier = System.currentTimeMillis()
    recommendDS.write.option("header","true").csv(s"output/recommendation-${identifier}") //need to find out how to view parquet

    bestModelFromCR.save(s"output/cv-model-${identifier}") // It is using MLWriter
    val loadedCvModel = CrossValidatorModel.load(s"output/cv-model-${identifier}")
    assert(loadedCvModel != null)

  }

  def getBestCrossValidatorModel(cv: CrossValidator,
                                 mrDS: Dataset[Rating], prDS: Dataset[Rating]): CrossValidatorModel = {
    val Array(cvTrainValDS, cvTestDS) = mrDS.randomSplit(Array(0.9, 0.1))
    val cvTrainValPlusDS = cvTrainValDS.union(prDS).cache()

    val cvModel = cv.fit(cvTrainValPlusDS)
    val bestRmse = cv.getEvaluator.evaluate(cvModel.transform(cvTrainValPlusDS))
    val cvPrediction = cvModel.transform(cvTestDS)
    val cvRmse = cv.getEvaluator.evaluate(cvPrediction)
    //It only return base and does not give us extra params like regParam or rank
    printf("The RMSE of the bestModel from CrossValidator on validation set is %3.4f\n", bestRmse)
    printf("The RMSE of the bestModel from CrossValidator on test set is %3.4f\n", cvRmse)
    println()
    cvModel
  }


}
