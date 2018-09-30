import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.Partitioner;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class Friends {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").config("", "").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		
		JavaRDD<String> lines = spark.read().textFile("sociNetShort.txt").javaRDD();
		
//		JavaRDD<String> tokenized = lines.flatMap(line -> Arrays.asList(line.split("\t")).iterator());
//		JavaRDD<String> noFriends = tokenized.filter(l->l.length() > 1);
		
//		System.out.println("tokenized count: " + tokenized.count());
//		System.out.println("noFriends count: " + noFriends.count());

//		System.out.println(tokenized.count());

		//splits row into integer of person and comma string of friends/null if no friends
	    JavaPairRDD<Integer, String> intCommaStrFriends = lines.mapToPair(
    		new PairFunction<String, Integer, String>(){
    			public Tuple2<Integer, String> call(String s){
    				String[] strArr = s.split("\t");
    				return new Tuple2(Integer.parseInt(strArr[0]), (strArr.length > 1) ? strArr[1] : "");
    			}
    		}
    	);
	    JavaPairRDD<Integer, String> noFriends = intCommaStrFriends.filter(t->t._2 != "");
	    JavaPairRDD<Integer, String> hasFriends = intCommaStrFriends.filter(t->t._2 == "");
	    
	    System.out.println("intCommaStrFriends count: " + intCommaStrFriends.count());
	    System.out.println("noFriends count: " + noFriends.count());
	    System.out.println("hasFriends count: " + hasFriends.count());
//	    JavaRDD<String> noFriends = intCommaStrFriends.flatMap(
//	    		new FlatMapFunction<Tuple2<Integer, String>, String>() {
//	    			String call(Tuple2<Integer, String> t) {
//	    				
//	    			}
//	    		}
//	    )
		
	    
//	    JavaRDD<Integer> noFriends = intCommaStrFriends.flatMap(
//	    	new FlatMapFunction<Pair, String>
//	    		)
	    
//	    System.out.println(counts.count());

				
				
				
	}
}
