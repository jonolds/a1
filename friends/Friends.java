import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

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
import org.apache.spark.sql.SparkSession;

//import scala.Option;
//import scala.collection.Map;
//import scala.collection.immutable.List;
import scala.Serializable;
import scala.Tuple2;


public class Friends {
	public static void main(String[] args) throws IOException, InterruptedException {
		JavaRDD<String> lines = settings().read().textFile("sociNet.txt").javaRDD();
		
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
	
		
	//SWAP pers and numrecs
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> new_order = swap_1_3(deg2);
	//SORT By Count
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> sorted_by_count = new_order.sortByKey(new CompByCount2());
	//SWAP pers and numrecs back and switch format
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> sorted_new_format = swap_1_3_and_format(sorted_by_count);
	//group by key (make the collection in the form of an interable)
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> grouped_by_key = sorted_new_format.groupByKey();
	//Drop the count and convert iterable to array
		JavaPairRDD<Integer, Integer[]> ordered_suggests = iter2Array(grouped_by_key);
	//Add no friends list AND Order by Key
		JavaPairRDD<Integer, Integer[]> with_friendless_sorted = ordered_suggests.union(no_frds).sortByKey().repartition(1);
		
	//save it to a single line JavaRDD string
		JavaRDD<String> lines_out = ints2String(with_friendless_sorted);
		
		
		lines_out.saveAsTextFile("output/lines_out");
//		grouped_by_key.saveAsTextFile("output/grouped_by_key");
//		ordered_suggests.saveAsTextFile("output/ordered_suggests");
//		with_friendless_sorted.saveAsTextFile("output/final_int_rdd");
		Thread.sleep(60000);
	}
	
	static JavaRDD<String> ints2String(JavaPairRDD<Integer, Integer[]> int_intArr) {
		return int_intArr.map(new Function<Tuple2<Integer, Integer[]>, String>() {
			public String call(Tuple2<Integer, Integer[]> t) throws Exception {
				int i = 0;
				String s = t._1 + "\t";
				if(t._2 != null) {
					if(t._2.length >=1) {
						s += String.valueOf(t._2[i]);
						i++;
					}
					while(i < 10) {
						s+= (i < t._2.length) ? ("," + t._2[i]) : ", ";
						i++;
					}
				}
				else
					s += " , , , , , , , , , ";
				return s;
			}
			
		});
	}
	
	static void printFinal(Tuple2<Integer, Integer[]> t) {
		System.out.print(t._1);
		int i = 0;
		String s = "\t";
		if(t._2 != null) {
			if(t._2.length >=1) {
				s += String.valueOf(t._2[i]);
				i++;
			}
			while(i < 10) {
				s+= (i < t._2.length) ? ("," + t._2[i]) : ", ";
				i++;
			}
		}
		else
			s += " , , , , , , , , , ";
			
		System.out.println(s);
	}
	
	static JavaPairRDD<Integer, Integer[]> iter2Array(JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> int_TupItt_rdd) {
		
		JavaPairRDD<Integer, Integer[]> int_intArr = int_TupItt_rdd.mapToPair(new PairFunction<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>, Integer, Integer[]>() { 
			public Tuple2<Integer, Integer[]> call(Tuple2<Integer,Iterable<Tuple2<Integer,Integer>>> int_TupItt) {
				Iterator<Tuple2<Integer, Integer>> it = int_TupItt._2.iterator();
				List<Integer> suggests = new ArrayList<>();
				int i = 0;
				while(it.hasNext()) {
					suggests.add(it.next()._1);
					i++;
				}
				
				Integer[] intArr = suggests.toArray(new Integer[suggests.size()]);
//				for(Integer in: intArr)
//					System.out.println(in);
				return new Tuple2<Integer, Integer[]>(int_TupItt._1, intArr);
			}
		});
		return int_intArr;
	}
	
	//Swap new to old
	static JavaPairRDD<Tuple2<Integer, Integer>,Integer> swap_1_3(JavaPairRDD<Tuple2<Integer, Integer>,Integer> unsorted) {
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = unsorted.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>,Integer>, Tuple2<Integer, Integer>, Integer>() {
			public Tuple2<Tuple2<Integer, Integer>,Integer> call(Tuple2<Tuple2<Integer, Integer>,Integer> t) {
				return new Tuple2<>(new Tuple2<>(t._2, t._1._2), t._1._1);
			}
		});
		return swapped;
	}
	
	//Swap to new style
	static JavaPairRDD<Integer, Tuple2<Integer, Integer>> swap_1_3_and_format(JavaPairRDD<Tuple2<Integer, Integer>,Integer> unsorted) {
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> swapped = unsorted.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>,Integer>, Integer, Tuple2<Integer, Integer>>() {
			public Tuple2<Integer, Tuple2<Integer,Integer>> call(Tuple2<Tuple2<Integer, Integer>,Integer> t) {
				return new Tuple2<>(t._2, new Tuple2<>(t._1._2, t._1._1));
			}
		});
		return swapped;
	}
	
//	static JavaPairRDD<Integer, Tuple2<Integer,Integer>> sort(JavaPairRDD<Integer, Tuple2<Integer,Integer>> unsorted) {
//		//Swap key._2 with value
//		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = swap_T2_Int(unsorted);
//		//Sort by key
//		JavaPairRDD<Tuple2<Integer, Integer>,Integer> sorted_but_swapped = swapped.sortByKey(new CompByCount2());
//		//Unswap
//		JavaPairRDD<Integer, Tuple2<Integer, Integer>> sorted = swap_Int_T2(sorted_but_swapped);
//		return sorted;
//	}
	
	//Swap new to old
	static JavaPairRDD<Tuple2<Integer, Integer>,Integer> swap_T2_Int(JavaPairRDD<Integer, Tuple2<Integer, Integer>> unsorted) {
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> swapped = unsorted.mapToPair(new PairFunction< Tuple2<Integer, Tuple2<Integer,Integer>>, Tuple2<Integer, Integer>, Integer>() {
			public Tuple2<Tuple2<Integer, Integer>,Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> t2) {
				return new Tuple2<>(new Tuple2<>(t2._1, t2._2._1), t2._2._2);
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
			return (a._1 > b._1) ? -1 : (a._1 < b._1) ? 1 : (a._2 > b._2) ? 1 : (a._2 < b._2) ? -1 : 0;
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
			return (a._1 > b._1) ? -1 : (a._1 < b._1) ? 1 : (a._2 > b._2) ? 1 : (a._2 < b._2) ? -1 : 0;
		}
	}
	
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
	static void printCounts(Object[] objs) {
		System.out.println("\n" +"Counts:");
		for(int i = 0; i < objs.length; i+=2) {
			System.out.println("    " + objs[i] + ((JavaRDDLike)objs[i+1]).count());
		}
	}
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