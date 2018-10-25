package scala_advance
//import org.apache.spark._
//import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils

object twitter_tweet {

  def main(args: Array[String]): Unit = {

    //val session = SparkSession.builder().appName("twitter").master("local").getOrCreate()
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val consumerKey= "s4Jlz5yGTBMTETPMzrWlivUlc"
    val consumerSecret= "Nnsch5M2fekJlHI7tmU6zSKudywUEyBAnrptsl1rYTxMJDSfS9"
    val accessToken = "863219944300756995-HtGYFbO7fO3mrCepTmmrZQ9d3cAER0t"
    val accessTokenSecret= "rKrO88snh5uBVti509ytWpCAgZSOqTUGywsZxsGbN3sir"

    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)


    val tweets = TwitterUtils.createStream(ssc, Some(auth))

    val englishTweets = tweets.filter(_.getLang() == "en")
    englishTweets.print()
    //englishTweets.saveAsTextFiles("tweets", "json")
    // println(englishTweets)
    ssc.start()
    ssc.awaitTermination()


  }


}
