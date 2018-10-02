import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

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

import scala.Option;
import scala.Serializable;
import scala.Tuple2;
//import scala.collection.Map;
//import scala.collection.immutable.List;

@SuppressWarnings("unused")
public class Friends {	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		SparkSession spark = settings();
		//JAVARDD w/ each element a string in format: [person] + "\t" + [comma separated friends}
		JavaRDD<String> lines = spark.read().textFile("sociNet.txt").javaRDD();
		
		//split each line into String[] with regex="\t". All, even friendless, have tabs.
		JavaRDD<String[]> tokenized = lines.map(new Function<String, String[]>() {
			public String[] call(String s) {
				return s.split("\t");
			}
		});
		
		//make a pair for each person that has friends with an (person) and [](friends).
		JavaPairRDD<String, String[]> has_friends = tokenized.filter(x->x.length > 1).mapToPair(new PairFunction<String[], String, String[]>() {
			public Tuple2<String, String[]> call(String[] strArr) {
				String[] frSA = strArr[1].split(",");
				String[] friends = new String[frSA.length];
				for(int i = 0; i < frSA.length; i++)
					friends[i] = frSA[i];
				String p = strArr[0];
				return new Tuple2<String, String[]>(p, friends);
			}
		});
			
		//make an  RDD for people with no friends
		JavaPairRDD<String, String> no_friends = tokenized.filter(x->x.length == 1).mapToPair(new PairFunction<String[], String, String>() {
			public Tuple2<String, String> call(String[] strArr) {
				return new Tuple2<>(strArr[0], " , , , , , , , , , ");
			}
		});
		
		//Create first degree relations. Saved using the lower person number as key;
		JavaPairRDD<Tuple2<String, String>,Integer> deg1 = has_friends.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, Tuple2<String, String>, Integer>() {
			public Iterator<Tuple2<Tuple2<String, String>,Integer>> call(Tuple2<String, String[]> t2) {
				List<Tuple2<Tuple2<String, String>, Integer>> pairs = new ArrayList<>();
				for(int i = 0; i < t2._2.length;  i++)
					pairs.add(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(t2._1, t2._2[i]), 1));
				return pairs.iterator();
			}
		});
		//deg1 method2
		JavaPairRDD<Tuple2<String, String>,Integer> deg1_method = has_friends.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, Tuple2<String, String>, Integer>() {
			public Iterator<Tuple2<Tuple2<String, String>,Integer>> call(Tuple2<String, String[]> t2) {
				List<Tuple2<Tuple2<String, String>, Integer>> pairs = new ArrayList<>();
				for(int i = 0; i < t2._2.length;  i++)
					pairs.add(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(t2._1, t2._2[i]), 1));
				return pairs.iterator();
			}
		});

		
		JavaPairRDD<Tuple2<String, String>,Integer> deg2_candidates = has_friends.filter(x->x._2 != null).flatMapToPair(new PairFlatMapFunction<Tuple2<String, String[]>, Tuple2<String, String>, Integer>() {
			public Iterator<Tuple2<Tuple2<String, String>,Integer>>  call(Tuple2<String, String[]> pers) {
				List<Tuple2<Tuple2<String, String>,Integer>> d2 = new ArrayList<Tuple2<Tuple2<String, String>,Integer>>();
				for(String friend : pers._2) {
					for(int i = 0; i < pers._2.length; i++)
						if(pers._2[i] != friend)
							d2.add(new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(friend, pers._2[i]), 1));
				}
				return d2.iterator();
			}
		});
		System.out.println("deg2.count: " + deg2_candidates.count());
//		System.out.println("deg2_2.count: " + deg2_candidates2.count());
		
		
		
//		JavaPairRDD<Tuple2<String, String>,String> list_before_sum = deg2_candidates.subtract(deg1);
		JavaPairRDD<Tuple2<String, String>,Integer> list_summed = deg2_candidates.reduceByKey((i1, i2) -> i1 + i2).subtract(deg1);
		JavaPairRDD<Tuple2<String, String>,Integer> sorted = list_summed.sortByKey(new Comp());
		List<Tuple2<Tuple2<String, String>, Integer>> aList2 = sorted.collect();
		sorted.saveAsTextFile("output");
		Thread.sleep(120000);
	}
	static class Comp implements Comparator<Tuple2<String, String>>, Serializable {
		public int compare(Tuple2<String, String> a, Tuple2<String, String> b) {
			String[] iArr = new String[] {a._1(), b._1(), a._2(), b._2()};
			for(int i = 0; i < 4; i+=2) {
				if(Integer.parseInt(iArr[i]) < Integer.parseInt(iArr[i+1]))
					return -1;
				if(Integer.parseInt(iArr[i]) > Integer.parseInt(iArr[i+1]))
					return 1;
			}
			return 0;
		}
	}
	
	static SparkSession settings() throws IOException {
//		Logger.getLogger("org").setLevel(Level.WARN);
//		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").config("spark.eventlog.enabled","true").getOrCreate();
		SparkContext sc = spark.sparkContext();
		for(Tuple2<String, String> t2: sc.conf().getAll())
			System.out.println(t2._1 + ",   " + t2._2);
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
		
		
		
		
//	public static void main(String[] args) throws IOException, InterruptedException {
//		SparkSession spark = settings();
//		//JAVARDD w/ each element a string in format: [person] + "\t" + [comma separated friends}
//		JavaRDD<String> lines = spark.read().textFile("sociNet.txt").javaRDD();
//		
//		//split each line into String[] with regex="\t". All, even friendless, have tabs.
//		JavaRDD<String[]> tokenized = lines.map(new Function<String, String[]>() {
//			public String[] call(String s) {
//				return s.split("\t");
//			}
//		});
//		//make an Integer RDD for people with no friends
//		JavaRDD<Integer> no_friends = tokenized.filter(x->x.length == 1).map(new Function<String[], Integer>() {
//			public Integer call(String[] strArr) {
//				return Integer.parseInt(strArr[0]);
//			}
//		});
//		//make a pair for each person that has friends with an Integer(person) and Integer[](friends).
//		JavaPairRDD<Integer, Integer[]> has_friends = tokenized.filter(x->x.length > 1).mapToPair(new PairFunction<String[], Integer, Integer[]>() {
//			public Tuple2<Integer, Integer[]> call(String[] strArr) {
//				String[] frSA = strArr[1].split(",");
//				Integer[] friends = new Integer[frSA.length];
//				for(int i = 0; i < frSA.length; i++)
//					friends[i] = Integer.parseInt(frSA[i]);
//				Integer p = Integer.parseInt(strArr[0]);
//				return new Tuple2<Integer, Integer[]>(p, friends);
//			}
//		});
//		//make a pair for EVERY person with an Integer(person) and Integer[](friends)/null(no friends).
//		JavaPairRDD<Integer, Integer[]> people = tokenized.mapToPair(new PairFunction<String[], Integer, Integer[]>() {
//			public Tuple2<Integer, Integer[]> call(String[] strArr) {
//				if(strArr.length > 1) {
//					String[] frSA = strArr[1].split(",");
//					Integer[] friends = new Integer[frSA.length];
//					for(int i = 0; i < frSA.length; i++)
//						friends[i] = Integer.parseInt(frSA[i]);
//					Integer p = Integer.parseInt(strArr[0]);
//					return new Tuple2<Integer, Integer[]>(p, friends);
//				}
//				else
//					return new Tuple2<Integer, Integer[]>(Integer.parseInt(strArr[0]), null);
//			}
//		});
//		
//		//Create first degree relations. Saved using the lower person number as key;
//		JavaPairRDD<Tuple2<Integer, Integer>,Integer> deg1 = has_friends.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Integer[]>, Tuple2<Integer, Integer>, Integer>() {
//			public Iterator<Tuple2<Tuple2<Integer, Integer>,Integer>> call(Tuple2<Integer, Integer[]> t2) {
//				List<Tuple2<Tuple2<Integer, Integer>,Integer>> pairs = new ArrayList<>();
//				for(int i = 0; i < t2._2.length;  i++)
//					pairs.add(new Tuple2<>(new Tuple2<>(t2._1, t2._2[i]), 1));
//				return pairs.iterator();
//			}
//		});
//		
//		JavaPairRDD<Tuple2<Integer, Integer>,Integer> deg2_candidates = people.filter(x->x._2 != null).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Integer[]>, Tuple2<Integer, Integer>, Integer>() {
//			public Iterator<Tuple2<Tuple2<Integer, Integer>,Integer>>  call(Tuple2<Integer, Integer[]> pers) {
//				List<Tuple2<Tuple2<Integer, Integer>,Integer>> d2 = new ArrayList<Tuple2<Tuple2<Integer, Integer>,Integer>>();
//				for(Integer friend : pers._2) {
//					for(int i = 0; i < pers._2.length; i++)
//						if(pers._2[i] != friend)
//							d2.add(new Tuple2<>(new Tuple2<>(friend, pers._2[i]), 1));
//				}
//				return d2.iterator();
//			}
//		});
////		JavaPairRDD<Tuple2<Integer, Integer>,Integer> list_before_sum = deg2_candidates.subtract(deg1);
//		JavaPairRDD<Tuple2<Integer, Integer>,Integer> list_summed = deg2_candidates.reduceByKey((i1, i2) -> i1 + i2).subtract(deg1);
//		JavaPairRDD<Tuple2<Integer, Integer>,Integer> sorted = list_summed.sortByKey(new Comp());
//		List<Tuple2<Tuple2<Integer, Integer>, Integer>> aList2 = sorted.collect();
//		sorted.saveAsTextFile("output");
//		Thread.sleep(120000);
//	}
//	
//	static class Comp implements Comparator<Tuple2<Integer, Integer>>, Serializable {
//		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
//			Integer[] iArr = new Integer[] {a._1(), b._1(), a._2(), b._2()};
//			for(int i = 0; i < 4; i+=2) {
//				if(iArr[i] < iArr[i+1])
//					return -1;
//				if(iArr[i] > iArr[i+1])
//					return 1;
//			}
//			return 0;
//		}
//	}
//	
//	static SparkSession settings() throws IOException {
////		Logger.getLogger("org").setLevel(Level.WARN);
////		Logger.getLogger("akka").setLevel(Level.WARN);
//		SparkSession.clearActiveSession();
//		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").config("spark.eventlog.enabled","true").getOrCreate();
//		SparkContext sc = spark.sparkContext();
//		for(Tuple2<String, String> t2: sc.conf().getAll())
//			System.out.println(t2._1 + ",   " + t2._2);
//		sc.setLogLevel("WARN");
//		FileUtils.deleteDirectory(new File("output"));
//		return spark;
//	}
	
	
	
	
	
}