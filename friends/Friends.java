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
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

//import scala.Option;
import scala.*;
//import scala.collection.Map;
//import scala.collection.immutable.List;


@SuppressWarnings("unused")
public class Friends {
	public static void main(String[] args) throws IOException, InterruptedException {
		JavaRDD<String> lines = settings().read().textFile("sociNetShort.txt").javaRDD();
		
		JavaPairRDD<Integer, Integer[]> tokenized = lines.mapToPair(new PairFunction<String, Integer, Integer[]>() { 
			public Tuple2<Integer, Integer[]> call(String s) {
				String[] pers_frds_split = s.split("\t");
				if(pers_frds_split.length > 1)
					return new Tuple2<>(Integer.parseInt(pers_frds_split[0]), Arrays.stream(pers_frds_split[1].split(",")).map(x->Integer.parseInt(x)).toArray(Integer[]::new));
				else
					return new Tuple2<>(Integer.parseInt(pers_frds_split[0]), null);
			} });
		
		JavaPairRDD<Integer, Integer[]> has_frds = tokenized.filter(x->x._2 != null);
		JavaPairRDD<Integer, Integer[]> no_frds = tokenized.filter(x->x._2 == null);
		
	//Deg_1 relations with tuple2<person, friend> as key, 0 as value for later subtraction
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> pers_fr_0 = has_frds.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Integer[]>, Tuple2<Integer, Integer>, Integer>() {
			public Iterator<Tuple2<Tuple2<Integer, Integer>,Integer>> call(Tuple2<Integer, Integer[]> t2) {
				List<Tuple2<Tuple2<Integer, Integer>,Integer>> pairs = new ArrayList<>();
				for(Integer s: t2._2)
					pairs.add(new Tuple2<>(new Tuple2<Integer, Integer>(t2._1, s), 0));
				return pairs.iterator();
			} });
		
	//Deg2 possibles. Uses has_frds to create a deg2_poss_1 entry for each pair of frds with val=1
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> deg2_poss_1 = has_frds.filter(x->x._2 != null).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Integer[]>, Tuple2<Integer, Integer>, Integer>() {
			public Iterator<Tuple2<Tuple2<Integer, Integer>,Integer>>  call(Tuple2<Integer, Integer[]> pers) {
				List<Tuple2<Tuple2<Integer, Integer>,Integer>> d2 = new ArrayList<>();
				for(Integer friend : pers._2)
					for(int i = 0; i < pers._2.length; i++)
						if(pers._2[i] != friend)
							d2.add(new Tuple2<>(new Tuple2<>(friend, pers._2[i]), 1));
				return d2.iterator();
			} });
		
	//Sum (reduce) the mutual friend counts
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> deg2_sum = deg2_poss_1.reduceByKey((i1, i2) -> i1 + i2);
	//For each person, remove the recommendations they're already friends with
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> deg2 = deg2_sum.subtractByKey(pers_fr_0);

		JavaPairRDD<Integer, Tuple2<Integer,Integer>> keys_split = swap_to_Int_T2(deg2);

		keys_split.saveAsTextFile("output/keys_split");
		
		Thread.sleep(60000);
	}
	
	static JavaPairRDD<Integer, Tuple2<Integer,Integer>> sort(JavaPairRDD<Integer, Tuple2<Integer,Integer>> unsorted) {
		//Swap key._2 with value
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = swap_to_T2_Int(unsorted);
		//Sort by key
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> sorted_but_swapped = swapped.sortByKey(new CompByCount());
		//Unswap
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> sorted = swap_to_Int_T2(sorted_but_swapped);
		return sorted;
	}
	
	static JavaPairRDD<Tuple2<Integer, Integer>,Integer> swap_to_T2_Int(JavaPairRDD<Integer, Tuple2<Integer, Integer>> unsorted) {
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = unsorted.mapToPair(new PairFunction< Tuple2<Integer, Tuple2<Integer,Integer>>, Tuple2<Integer, Integer>, Integer>() {
			public Tuple2<Tuple2<Integer, Integer>,Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> t2) {
				return new Tuple2<>(new Tuple2<>(t2._1, t2._2._1), t2._1);
			}
		});
		return swapped;
	}
	
	static JavaPairRDD<Integer, Tuple2<Integer, Integer>> swap_to_Int_T2(JavaPairRDD<Tuple2<Integer, Integer>,Integer> unsorted) {
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> swapped = unsorted.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>,Integer>, Integer, Tuple2<Integer, Integer>>() {
			public Tuple2<Integer, Tuple2<Integer,Integer>> call(Tuple2<Tuple2<Integer, Integer>,Integer> t2) {
				return new Tuple2<>(t2._1._1, new Tuple2<>(t2._1._2, t2._2));
			}
		});
		return swapped;
	}
	
	static class CompPersFr2 implements Comparator<Tuple2<Integer, Integer>>, Serializable {
		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
			return (a._1 > b._1) ? 1: (a._1 < b._1) ? -1 : (a._2 > b._2) ? 1 : (a._2 < b._2) ? -1 : 0;
		}
	}
	static class CompByCount2 implements Comparator<Tuple2<Integer, Integer>>, Serializable {
		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
			return (a._1 > b._1) ? 1 : (a._1 < b._1) ? -1 : (a._2 > b._2) ? -1 : (a._2 < b._2) ? 1 : 0;
		}
	}
	
	static JavaPairRDD<Tuple2<Integer, Integer>,Integer> sortByCount(JavaPairRDD<Tuple2<Integer, Integer>,Integer> unsorted) {
		//Swap key._2 with value
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = swapKey2Val(unsorted);
		//Sort by key
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> sorted_but_swapped = swapped.sortByKey(new CompByCount());
		//Unswap
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> sorted = swapKey2Val(sorted_but_swapped);
		return sorted;
	}
	static JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapKey2Val(JavaPairRDD<Tuple2<Integer, Integer>,Integer> unsorted) {
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = unsorted.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>,Integer>, Tuple2<Integer, Integer>, Integer>() {
			public Tuple2<Tuple2<Integer, Integer>,Integer> call(Tuple2<Tuple2<Integer, Integer>,Integer> t2) {
				return new Tuple2<>(new Tuple2<>(t2._1._1, t2._2), t2._1._2);
			}
		});
		return swapped;
	}
	static class CompPersFr implements Comparator<Tuple2<Integer, Integer>>, Serializable {
		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
			return (a._1 > b._1) ? 1: (a._1 < b._1) ? -1 : (a._2 > b._2) ? 1 : (a._2 < b._2) ? -1 : 0;
		}
	}
	static class CompByCount implements Comparator<Tuple2<Integer, Integer>>, Serializable {
		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
			return (a._1 > b._1) ? 1 : (a._1 < b._1) ? -1 : (a._2 > b._2) ? -1 : (a._2 < b._2) ? 1 : 0;
		}
	}
	
	@SuppressWarnings("unchecked")
	static void printSortPersNumrecsFr(Object objs) throws InterruptedException {
		System.out.println("\n" +"sort_pers_numrecs_fr: ");
		List<Tuple2<Tuple2<String, Integer>, String>> aList2 = ((JavaPairRDD<Tuple2<String, Integer>,String>)objs).collect();
		for(Tuple2<Tuple2<String, Integer>, String> t2: aList2)
			System.out.println("	("+ t2._1._1 + "," + t2._2 + ") - " + t2._1._2);
		((JavaPairRDD<Tuple2<String, Integer>,String>)objs).saveAsTextFile("output");
	}
	static void printSave(Object[] objs) throws InterruptedException {
//		printSortPersFr(objs[0]);
//		printSortPersNumrecsFr(objs[1]);
		printCounts((Object[])objs[2]);
		
	}
	@SuppressWarnings("rawtypes")
	static void printCounts(Object[] objs) {
		System.out.println("\n" +"Counts:");
		for(int i = 0; i < objs.length; i+=2) {
			System.out.println("    " + objs[i] + ((JavaRDDLike)objs[i+1]).count());
		}
	}
	@SuppressWarnings("unchecked")
	static void printSortPersFr(Object sorted) throws InterruptedException {
		System.out.println("\n" +"sort_pers_fr: ");
		List<Tuple2<Tuple2<String, String>, Integer>> aList2 = ((JavaPairRDD<Tuple2<String, String>,Integer>)sorted).collect();
		for(Tuple2<Tuple2<String, String>, Integer> t2: aList2)
			System.out.println("	("+ t2._1._1 + "," + t2._1._2 + ") - " + t2._2);
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