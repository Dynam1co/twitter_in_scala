import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.{AccessToken, OAuthAuthorization}
import twitter4j.{Status, TwitterFactory}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming._

object test {
    def main(args: Array[String]): Unit = {
        val appName = "TwitterData"
        //create context
        val scparkConf = new SparkConf().setAppName(appName)
        scparkConf.setMaster("local")

        val ssc = new StreamingContext(scparkConf, Seconds(10))

        // values of Twitter API.
        val consumerKey = "" // Your consumerKey
        val consumerSecret = "" // your API secret
        val accessToken ="" // your access token
        val accessTokenSecret = "" // your token secret

        val twitter = new TwitterFactory().getInstance()
        twitter.setOAuthConsumer(consumerKey, consumerSecret)
        twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))

        val stream = TwitterUtils.createStream(ssc, None).window(Seconds(20))
        val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

        hashTags.saveAsTextFiles("./Tweets")
        //println(hashTags)

        ssc.start()
        ssc.awaitTermination()
    }
}
