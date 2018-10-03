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
		
		//make an  RDD for people with no friends
		JavaPairRDD<String, String[]> no_frds = tokenized.filter(x->x.length == 1).mapToPair(new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] strArr) {
				return new Tuple2<>(strArr[0], new String[] {"","","","","","","","","",""});
			}
		});
		System.out.println("no_frds.count: " + no_frds.count());
		
		//make a pair for each person that has friends with an (person) and [](friends).		
		JavaPairRDD<String, String[]> has_frds = tokenized.filter(x->x.length > 1).mapToPair(new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] strArr) {
				return new Tuple2<>(strArr[0], strArr[1].split(","));
			}
		});
		System.out.println("has_frds.count: " + has_frds.count());
		
		//Deg_1 relations with tuple2<person, friend> as key, 0 as value
		JavaPairRDD<Tuple2<String, String>,Integer> pers_fr_0 = has_frds.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, Tuple2<String, String>, Integer>() {
			public Iterator<Tuple2<Tuple2<String, String>,Integer>> call(Tuple2<String, String[]> t2) {
				List<Tuple2<Tuple2<String, String>,Integer>> pairs = new ArrayList<>();
				for(String s: t2._2)
					pairs.add(new Tuple2<>(new Tuple2<String, String>(t2._1, s), 0));
				return pairs.iterator();
			}
		});
		System.out.println("pers_fr_0.count: " + pers_fr_0.count());
		
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
		System.out.println("deg2_poss_1.count: " + deg2_poss_1.count());
				
		JavaPairRDD<Tuple2<String, String>,Integer> deg2_poss_sum = deg2_poss_1.reduceByKey((i1, i2) -> i1 + i2);
		System.out.println("deg2_poss_sum.count: " + deg2_poss_sum.count());
		JavaPairRDD<Tuple2<String, String>,Integer> deg2 = deg2_poss_sum.subtractByKey(pers_fr_0);
		System.out.println("deg2.count: " + deg2.count());
		JavaPairRDD<Tuple2<String, String>,Integer> sort_pers_fr = deg2.sortByKey(new CompPersFr());
		System.out.println("sorted.count: " + sort_pers_fr.count());
		
		JavaPairRDD<Tuple2<String, Integer>,String> sort_pers_numrecs_fr = sort_pers_fr.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>,Integer>, Tuple2<String, Integer>, String>() {
			public Tuple2<Tuple2<String, Integer>,String> call(Tuple2<Tuple2<String, String>,Integer> t2t2) {
				return new Tuple2<>(new Tuple2<>(t2t2._1._1, t2t2._2), t2t2._1._2);
			}
		}).sortByKey(new CompByCount());

		System.out.println("printSave(sort_pers_fr)");
		printSave(sort_pers_fr);
		System.out.println();
		System.out.println("printSave2(sort_pers_numrecs_fr)");
		printSave2(sort_pers_numrecs_fr);
		
	}
	
	static void printSave(JavaPairRDD<Tuple2<String, String>,Integer> sorted) throws InterruptedException {
		List<Tuple2<Tuple2<String, String>, Integer>> aList2 = sorted.collect();
		for(Tuple2<Tuple2<String, String>, Integer> t2: aList2)
			System.out.println("("+ t2._1._1 + "," + t2._1._2 + ") - " + t2._2);
//		sorted.saveAsTextFile("output");
//		Thread.sleep(180000);	//Leave spark Web Gui available for 2 mins to look at results
	}
	
	static void printSave2(JavaPairRDD<Tuple2<String, Integer>,String> sorted) throws InterruptedException {
		List<Tuple2<Tuple2<String, Integer>, String>> aList2 = sorted.collect();
		for(Tuple2<Tuple2<String, Integer>, String> t2: aList2)
			System.out.println("("+ t2._1._1 + "," + t2._2 + ") - " + t2._1._2);
		sorted.saveAsTextFile("output");
//		Thread.sleep(180000);	//Leave spark Web Gui available for 2 mins to look at results
	}
	
	static class CompPersFr implements Comparator<Tuple2<String, String>>, Serializable {
		public int compare(Tuple2<String, String> a, Tuple2<String, String> b) {
			return (s2int(a._1) > s2int(b._1)) ? 1: (s2int(a._1) < s2int(b._1)) ? -1 : (s2int(a._2) >s2int( b._2)) ? 1 : (s2int(a._2) < s2int(b._2)) ? -1 : 0;
		}
	}
	
	static class CompByCount implements Comparator<Tuple2<String, Integer>>, Serializable {
		public int compare(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
			return (s2int(a._1) > s2int(b._1)) ? 1 : (s2int(a._1) < s2int(b._1)) ? -1 : (a._2 > b._2) ? -1 : (a._2 < b._2) ? 1 : 0;
		}
	}
	
	static Integer s2int(String s) {
		return Integer.parseInt(s);
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