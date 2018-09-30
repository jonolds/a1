
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;




public class Friends extends Object {
//	Tuple2<Integer, String>[] test(int a, Str)
//	System.out.println(lines.count());
//	System.out.println(a.count());
//	JavaRDD<String> noFriends = lines.filter(line->line.charAt(line.length()-1) == '\t');
//	System.out.println(noFriends.count());
//	<String[]> b = a.map(str->Arrays.stream(str.split(",")).filter(str.length > 1));
//	Stream<String> noFriends = b.map(str->Stream(str, 0, str.length));
//	for(String s: c)
//		System.out.println(c);
//	System.out.println(noFriends.count());
//	List<String[]> b = a.map(str->str.split(",")).collect();
//	
//	val q = str->mapInt(i2->Integer.parseInt(i2));
//	JavaRDD<String> a = lines.flatMap(line->Arrays.stream(strToStrArr(line)).iterator());
//	JavaRDD<String> noFriends = a.filter(i->Arrays.asList(i).size() <= 1);
//	JavaRDD<String> hasFriends = a.filter(i->Arrays.asList(i).size() > 1);
//	System.out.println(c.count());
//	JavaPairRDD<Integer, String> indexPair = lines.mapToPair(x->new Tuple2<>(x.indexOf("\t"), x));
//	JavaPairRDD<Integer, String> intStrPair = indexPair.mapToPair(p->new Tuple2<>(Integer.valueOf(p._2.substring(0, p._1)), p._2.substring(p._1 + 1)));
//	JavaPairRDD<Integer, Integer[]> intIntPair = intStrPair.mapToPair(p->new Tuple2<>(p._1, commaStrToIntArr(p._2)));
//	JavaPairRDD<Integer, String[]> intStrArrPair = intStrPair.mapToPair(p->new Tuple2<>(p._1, p._2.split(",")));
//	JavaPairRDD<Integer, Integer> test2;
//	intStrArrPair.map(p->Arrays.asList(p._2)).forEach(num->test2.collect(new Tuple2<Integer, Integer>(Integer.parseInt(num), p._1));
//	intStrArrPair.foreach(pair->Arrays.stream(pair._2));
//	JavaPairRDD<Integer, String> test = (intStrArrPair.flatMap(pair->(pair._2)));
//	JavaPairRDD<Integer, Integer> test = intStrArrPair.map(p->Arrays.stream(p._2).forEach(q, new Tuple2<>(p._1, Integer.parseInt(q))));
//	System.out.println(strPair.first()._1);
//	System.out.println(strPair.first()._2);
//	JavaPairRDD<Integer, String> intStrPair = indexPair.mapToPair(x->new Tuple2<>(Integer.valueOf(x._2.substring(0, x._1-1)), x._2.substring(x._1+1)));
//	JavaPairRDD<Integer, String> intStrPair = indexPair.mapToPair(x->new Tuple2<>(Integer.valueOf(x._2.substring(0, x._1-1)), x._2));
//	System.out.println(strPair.count());
//	System.out.println(intStrPair.count());
//	JavaPairRDD<String, String> strPairs = words.mapToPair(w->)
//	JavaPairRDD<Integer, String> pairs = words.mapToPair(x->new Tuple2<>(Integer.parseInt(x[0]), x[1]));
//	spark.stop();
//	JavaRDD<String>
//	JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(Pattern.compile("").split(s)).iterator());
//	JavaPairRDD<Integer, Integer[]> ones = lines.mapToPair(s -> new Tuple2<>(s, 1));

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession spark = SparkSession.builder().appName("JavaWordCount").config("spark.master", "local").config("", "").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
//		Friends.run(args, spark);
		
		
		JavaRDD<String> lines = spark.read().textFile("sociNetShort.txt").javaRDD();

		JavaPairRDD<String, String> tokenized = lines.flatMap(
				new PairFunction() {
					public Iterable call(String s) {
						return Arrays.asList(s.split("\t"));
					}

					@Override
					public Tuple2 call(Object arg0) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				};
	}
}
