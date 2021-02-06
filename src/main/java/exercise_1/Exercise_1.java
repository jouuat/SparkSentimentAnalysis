package exercise_1;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple5;
import twitter4j.Status;

import java.util.List;

public class Exercise_1 {

	// (tweet ID, tweet txt)
	public static void sentimentAnalysis(JavaDStream<Status> statuses) {
		JavaPairDStream<Long, String> tweets =  statuses.mapToPair(status ->
				new Tuple2<>(status.getId(), status.getText().replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase()));

		//(filter english words) (tweet ID, tweet txt)
		JavaPairDStream<Long, String> englishTweets = tweets.filter(tweet-> LanguageDetector.isEnglish(tweet._2));

		//(filter covid tweets) (tweet ID, tweet txt)
		JavaPairDStream<Long, String> coronaTweets = englishTweets.filter(tweet->{
			String[] words = tweet._2.split(" ");
			for (String word : words) {
				if (word.contains("covid")) {
					return true;
				}else if(word.contains("coronavirus")) {
					return true;
				}
			}
			return false;
		});

		/*
		// Print
		coronaTweets.foreachRDD(rdd -> {
			String out = "\njust corona tweets\n" ;
			for (Tuple2<Long, String> t : rdd.take(10)) {
				out = out + t.toString() + "\n";
			}
			System.out.println(out);
		});
		 */


		//remove stop words (ID, text)
		JavaPairDStream<Long, String> noStopTweets = coronaTweets.mapToPair(tweet -> {
			String text = tweet._2;
			List<String> stopWords = StopWords.getWords();
			for (String word : stopWords) {
				text = text.replaceAll("\\b" + word + "\\b", "");
			}
			return new Tuple2<Long, String>(tweet._1(), text);
		});

		//get the positive score (K:(ID), V:(text, score))
		JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets = noStopTweets.mapToPair(tweet -> {
			String[] words = tweet._2.split(" ");
			String positiveWords = PositiveWords.getWords().toString();
			Integer p = 0;
			Integer total = 0;
			for (String word : words) {
				total = total + 1;
				if (positiveWords.contains(word)) {
					p = p + 1;
				}
			}
			Float posScore = (float) p/total;
			return new Tuple2<Tuple2<Long, String>, Float>(new Tuple2<Long, String>( tweet._1, tweet._2), posScore);
		});

		//get the positive score (K:(ID), V:(text, score))
		JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets = noStopTweets.mapToPair(tweet -> {
			String[] words = tweet._2.split(" ");
			String negativeWords = NegativeWords.getWords().toString();
			Integer n = 0;
			Integer total = 0;
			for (String word : words) {
				total = total + 1;
				if (negativeWords.contains(word)) {
					n = n + 1;
				}
			}
			Float negScore = (float) n/total;
			return new Tuple2<Tuple2<Long, String>, Float>(new Tuple2<Long, String>( tweet._1, tweet._2), negScore);
		});


		//join positive and negative tweets
		JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined = positiveTweets.join( negativeTweets);

		//classify it
		JavaDStream<Tuple5<Long, String, Float, Float, String>> classified = joined.map(tweet -> {
				String classification;
				if (tweet._2._1 > tweet._2._2){
					classification = "positive";
				}else if (tweet._2._1 < tweet._2._2){
					classification = "negative";
				}else{
					classification = "neutral";
				}
				return new Tuple5<Long, String, Float, Float, String>(tweet._1._1 , tweet._1._2, tweet._2._1, tweet._2._2, classification);
			});


		// Print
		classified.foreachRDD(rdd -> {
			String out = "\ncorona Tweets\n" ;
			for (Tuple5<Long, String, Float, Float, String> t : rdd.take(10)) {
				out = out + t.toString() + "\n";
			}
			System.out.println(out);
		});


	}
}
