import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.Partitioner;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


@SuppressWarnings("unused")
public class Friends {
	
	public static void main(String[] args) {
		//JAVARDD w/ each element a string in format: [person] + "\t" + [comma separated friends}
		JavaRDD<String> lines = settings().read().textFile("sociNetShort.txt").javaRDD();
		
		//intCommaStrFriends: JavaPairRDD w/ each element a Tuple2<Integer, String> with [person] now converted to integer
	    JavaPairRDD<Integer, String> intCommaStrFriends = lines.mapToPair(
    		new PairFunction<String, Integer, String>(){
    			public Tuple2<Integer, String> call(String s){
    				String[] strArr = s.split("\t");
    				return new Tuple2<>(Integer.parseInt(strArr[0]), (strArr.length > 1) ? strArr[1] : "");
    			}
    		}
    	);
	    
	    
	    //noFriends: JavaPairRDD consisting of all intCommaStrFriends items with empty friends string
	    JavaPairRDD<Integer, String> noFriends = intCommaStrFriends.filter(t->t._2 == "");
	    
	    
	    
	    JavaRDD<Integer> no2 = noFriends.map(
	    	new Function<Tuple2<Integer, String>, Integer>() {
	    		public Integer call(Tuple2<Integer, String> t2) {
	    			return t2._1;
	    		}
	    	}
	    );
	    
	    
	    
	    
	    
	    //hasFriends: complement to noFriends JavaPairRDD. noFriends.count() + hasFriends.count() == intCommaStrFriends
	    JavaPairRDD<Integer, String> hasFriends = intCommaStrFriends.filter(t->t._2 != "");
	    
	    System.out.println("intCommaStrFriends count: " + intCommaStrFriends.count());
	    System.out.println("noFriends count: " + noFriends.count());
	    System.out.println("no2 count: " + no2.count());
	    System.out.println("hasFriends count: " + hasFriends.count());
	    
	    JavaPairRDD<Integer, Integer[]> friendsIntArr = hasFriends.mapToPair(
    		new PairFunction<Tuple2<Integer, String>, Integer, Integer[]>(){
    			public Tuple2<Integer, Integer[]> call(Tuple2<Integer, String> t){
    				String[] strArr = t._2.split(",");
    				Integer[] iArr = new Integer[strArr.length];
    				for(int i = 0; i < strArr.length; i++)
    					iArr[i] = Integer.parseInt(strArr[i]);
    				return new Tuple2<>(t._1, iArr);
    			}
    		}
    	);
	    System.out.println("friendsIntArr count: " + friendsIntArr.count());
	    System.out.println("friendsIntArr.first()._2.length: " + friendsIntArr.first()._2.length);

	}
	
	static SparkSession settings() {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").config("", "").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		return spark;
	}
}
