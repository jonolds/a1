import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

//import scala.Option;
import scala.Serializable;
import scala.*;
//import scala.collection.Map;
//import scala.collection.immutable.List;

@SuppressWarnings("unused")
public class Friends {	
	public static void main(String[] args) throws IOException, InterruptedException {
		SparkSession spark = settings();
		JavaRDD<String> lines = spark.read().textFile("sociNetShort.txt").javaRDD();
		JavaRDD<String[]> tokenized = lines.map(new Function<String, String[]>() { public String[] call(String s) { return s.split("\t"); } });
		
		//make a pair for each person that has friends with an (person) and [](friends).		
		JavaPairRDD<String, String[]> has_frds = tokenized.filter(x->x.length > 1).mapToPair(new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] strArr) {
				return new Tuple2<>(strArr[0], strArr[1].split(","));
			}
		});
		System.out.println("has_frds.count(): " + has_frds.count());
				
//		//Each entry is a person and each value is one of its friends
//		JavaPairRDD<String, String> pers_fr = has_frds.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, String, String>() {
//			public Iterator<Tuple2<String, String>> call(Tuple2<String, String[]> t2) {
//				List<Tuple2<String, String>> pairs = new ArrayList<>();
//				for(String s: t2._2)
//					pairs.add(new Tuple2<>(t2._1, s));
//				return pairs.iterator();
//			}
//		});
//		System.out.println("pers_fr.count(): " + pers_fr.count());
		
		//Each entry is a person and each value is one of its friends
		JavaPairRDD<Tuple2<String, String>,Integer> pers_fr_0 = has_frds.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, Tuple2<String, String>, Integer>() {
			public Iterator<Tuple2<Tuple2<String, String>,Integer>> call(Tuple2<String, String[]> t2) {
				List<Tuple2<Tuple2<String, String>,Integer>> pairs = new ArrayList<>();
				for(String s: t2._2)
					pairs.add(new Tuple2<>(new Tuple2<String, String>(t2._1, s), 0));
				return pairs.iterator();
			}
		});
		System.out.println("pers_fr_0.count(): " + pers_fr_0.count());
		
//		//Deg_1 relations with tuple2<person, friends> as key, 0 as value
//		JavaPairRDD<Tuple2<String, String>,Integer>  pers_fr_0 = pers_fr.mapToPair(new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
//			public Tuple2<Tuple2<String, String>,Integer> call(Tuple2<String, String> t1) {
//				return new Tuple2<>(new Tuple2<String, String>(t2._1, s), 0);
//			}
//		});
//		System.out.println("pers_fr_0.count(): " + pers_fr_0.count());
		
		//Deg2 possibles. Uses has_frds to create a deg2_poss_1 entry for each pair of frds with val=1
		JavaPairRDD<Tuple2<String, String>,Integer> deg2_poss_1 = has_frds.filter(x->x._2 != null).flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, Tuple2<String, String>, Integer>() {
			public Iterator<Tuple2<Tuple2<String, String>,Integer>>  call(Tuple2<String, String[]> pers) {
				List<Tuple2<Tuple2<String, String>,Integer>> d2 = new ArrayList<>();
				for(String friend : pers._2) {
					for(int i = 0; i < pers._2.length; i++)
						if(pers._2[i] != friend)
							d2.add(new Tuple2<>(new Tuple2<>(friend, pers._2[i]), 1));
				}
				return d2.iterator();
			}
		});
		System.out.println("deg2_poss_1.count(): " + deg2_poss_1.count());
		
		//make an  RDD for people with no friends
		JavaPairRDD<String, String[]> no_friends = tokenized.filter(x->x.length == 1).mapToPair(new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] strArr) {
				return new Tuple2<>(strArr[0], new String[] {"","","","","","","","","",""});
			}
		});
				
		JavaPairRDD<Tuple2<String, String>,Integer> list_summed = deg2_poss_1.reduceByKey((i1, i2) -> i1 + i2).subtract(pers_fr_0);
		JavaPairRDD<Tuple2<String, String>,Integer> sorted = list_summed.sortByKey(new Comp());
		List<Tuple2<Tuple2<String, String>, Integer>> aList2 = sorted.collect();
		
		
		
		sorted.saveAsTextFile("output");
		Thread.sleep(120000);	//Leave spark Web Gui available for 2 mins to look at results
	}
	static class Comp implements Comparator<Tuple2<String, String>>, Serializable {
		public int compare(Tuple2<String, String> a, Tuple2<String, String> b) {
			if(Integer.parseInt(a._1()) > Integer.parseInt(b._1()))
				return 1;
			else if(Integer.parseInt(a._1()) < Integer.parseInt(b._1()))
				return -1;
			else {
				if(Integer.parseInt(a._2()) >Integer.parseInt( b._2()))
					return 1;
				else if(Integer.parseInt(a._2()) < Integer.parseInt(b._2()))
					return -1;
				else
					return 0;
			}
		}
	}
	
	static class CompByCount implements Comparator<Tuple2<Tuple2<String, String>, Integer>>, Serializable {
		public int compare(Tuple2<Tuple2<String, String>, Integer> a, Tuple2<Tuple2<String, String>, Integer> b) {
			if(Integer.parseInt(a._1._1()) > Integer.parseInt(b._1._1()))
				return 1;
			else if(Integer.parseInt(a._1._1()) < Integer.parseInt(b._1._1()))
				return -1;
			else {
				if(a._2() > b._2())
					return 1;
				else if(a._2() < b._2())
					return -1;
				else {
					if(Integer.parseInt(a._1._2()) > Integer.parseInt(b._1._2()))
						return -1;
					else if(Integer.parseInt(a._1._2()) < Integer.parseInt(b._1._2()))
						return 1;
					else
						return 0;
				}	
			}
		}
	}
	
	static SparkSession settings() throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").config("spark.eventlog.enabled","true").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
}