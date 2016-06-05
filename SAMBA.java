import java.util.List;
import java.util.ArrayList;
import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;

/*
 * @author Md. Rezaul Karim - 05/06/2016
 * 
 */

@SuppressWarnings("unused")
public class FindAssociationRules {
	static JavaSparkContext createJavaSparkContext() throws Exception {
		SparkConf conf = new SparkConf().setAppName("market-basket-analysis").setMaster("local[4]")
				        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				        .set("spark.kryoserializer.buffer.mb", "32");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		return ctx;
	}
	static List<String> toList(String transaction) {
		String[] items = transaction.trim().split(",");
		List<String> list = new ArrayList<String>();
		for (String item : items) {
			list.add(item);
		}
		return list;
	}
	static List<String> removeOneItem(List<String> list, int i) {
		if ((list == null) || (list.isEmpty())) {
			return list;
		}
		if ((i < 0) || (i > (list.size() - 1))) {
			return list;
		}
		List<String> cloned = new ArrayList<String>(list);
		cloned.remove(i);
		return cloned;
	}

	public static void main(String[] args) throws Exception {
		// Step 2: handle input parameters
/*		if (args.length < 1) {
			System.err.println("Usage: FindAssociationRules <transactions>");
			System.exit(1);
		}
		St*/
		String transactionsFileName = "Input/data.txt";

		// Step 3: create a Spark context object
		JavaSparkContext ctx = createJavaSparkContext();
		// Step 4: read all transactions from HDFS and create the first RDD
		JavaRDD<String> transactions = ctx.textFile(transactionsFileName, 1);
		transactions.saveAsTextFile("Output/1");
		// Step 5: generate frequent patterns (map() phase 1)
		@SuppressWarnings("serial")
		JavaPairRDD<List<String>, Integer> patterns = transactions
				.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {
					public Iterable<Tuple2<List<String>, Integer>> call(String transaction) {
						List<String> list = toList(transaction);
						List<List<String>> combinations = Combination.findSortedCombinations(list);
						List<Tuple2<List<String>, Integer>> result = new ArrayList<Tuple2<List<String>, Integer>>();
						for (List<String> combList : combinations) {
							if (combList.size() > 0) {
								result.add(new Tuple2<List<String>, Integer>(combList, 1));
							}
						}
						return result;
					}
				});
		patterns.saveAsTextFile("Output/2");
		// Step 6: combine/reduce frequent patterns (reduce() phase 1)

		@SuppressWarnings("serial")
		JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		combined.saveAsTextFile("Output/3");
		// Step 7: generate all subpatterns (map() phase 2)

		@SuppressWarnings("serial")
		JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subpatterns = combined.flatMapToPair(
				new PairFlatMapFunction<Tuple2<List<String>, Integer>, List<String>, Tuple2<List<String>, Integer>>() {
					@SuppressWarnings({ "rawtypes", "unchecked" })
					public Iterable<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call(
							Tuple2<List<String>, Integer> pattern) {
						List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<Tuple2<List<String>, Tuple2<List<String>, Integer>>>();
						List<String> list = pattern._1;
						Integer frequency = pattern._2;
						result.add(new Tuple2(list, new Tuple2(null, frequency)));
						if (list.size() == 1) {
							return result;
						}

						// pattern has more than one item
						// result.add(new Tuple2(list, new Tuple2(null,size)));
						for (int i = 0; i < list.size(); i++) {
							List<String> sublist = removeOneItem(list, i);
							result.add(new Tuple2(sublist, new Tuple2(list, frequency)));
						}
						return result;
					}
				});
		subpatterns.saveAsTextFile("Output/4");
		// Step 8: combine subpatterns

		JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subpatterns.groupByKey();
		rules.saveAsTextFile("Output/5");
		// Step 9: generate association rules (reduce() phase 2)
		@SuppressWarnings("serial")
		JavaRDD<List<Tuple3<List<String>, List<String>, Double>>> assocRules = rules.map(
				new Function<Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>>, List<Tuple3<List<String>, List<String>, Double>>>() {
					@SuppressWarnings({ "rawtypes", "unchecked" })
					public List<Tuple3<List<String>, List<String>, Double>> call(
						Tuple2<List<String>, Iterable<Tuple2<List<String>, Integer>>> in) {
						List<Tuple3<List<String>, List<String>, Double>> result = new ArrayList<Tuple3<List<String>, List<String>, Double>>();
						List<String> fromList = in._1;
						Iterable<Tuple2<List<String>, Integer>> to = in._2;
						List<Tuple2<List<String>, Integer>> toList = new ArrayList<Tuple2<List<String>, Integer>>();
						Tuple2<List<String>, Integer> fromCount = null;
						for (Tuple2<List<String>, Integer> t2 : to) {
							// find the "count" object
							if (t2._1 == null) {
								fromCount = t2;
							} else {
								toList.add(t2);
							}
						}

						// now, we have the required objects for generating
						// association rules:
						// "fromList", "fromCount", and "toList"
						if (toList.isEmpty()) {
							// no output generated, but since Spark does not
							// like null objects, we will fake a null object
							return result; // an empty list
						}

						// now using 3 objects, "from", "fromCount", and
						// "toList",
						// create association rules:
						for (Tuple2<List<String>, Integer> t2 : toList) {
							double confidence = (double) t2._2 / (double) fromCount._2;
							List<String> t2List = new ArrayList<String>(t2._1);
							t2List.removeAll(fromList);
							result.add(new Tuple3(fromList, t2List, confidence));
						}
						return result;
					}
				});
		assocRules.saveAsTextFile("Output/6");
		System.exit(0);
	}
}