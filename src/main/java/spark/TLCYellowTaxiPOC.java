package spark;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class TLCYellowTaxiPOC {
	@SuppressWarnings("serial")
	public static void main (String[] args) {

		SparkConf conf = new SparkConf().setAppName("YellowTaxiPOC").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//sc.setLogLevel("ERROR");
		try {
		
		
		JavaRDD<String> lines = sc.textFile("D:\\VMs\\workspace\\spark\\src\\main\\resources\\trip_yellow_taxi.data");
		
		/**
		 * Task 1: Fetch the record having 
		 * VendorID as '2' AND 
		 * tpep_pickup_datetime as '2017-10-01 00:15:30' AND 
		 * tpep_dropoff_datetime as '2017-10-01 00:25:11' AND 
		 * passenger_count as '1' AND 
		 * trip_distance as '2.17'
		 */	
		System.out.println("Operation 1 [STARTS] : " + new Timestamp(System.currentTimeMillis()));
		JavaRDD<String> lines2 = lines.flatMap(
				new FlatMapFunction<String, String>() {

					/**
					 * FLAT MAP Example
					 */
					/*JavaRDD<String> result = data.flatMap(new FlatMapFunction<String, String>() {
						public Iterator<String> call(String s) {
						return Arrays.asList(s.split(" ")).iterator();
						} });*/
					
					@SuppressWarnings("unchecked")
					@Override
					public Iterator<String> call(String s) throws Exception {
						List<String> records = new ArrayList<String>();
						if (!s.isEmpty() && !s.equalsIgnoreCase("VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount")) {
							String[] vals = s.split(",");
							if ((Integer.parseInt(vals[0]) == 2) &&
									vals[1].equals("2017-10-01 00:15:30") &&
									vals[2].equals("2017-10-01 00:25:11") &&
									(Integer.parseInt(vals[3])) == 1 &&
									(Double.parseDouble(vals[4])) == 2.17) {
								records.add(s);
							}
						}
						return records.iterator();
					}
					
				});
		//lines2.foreach(x->System.out.println("line: " + x));
		System.out.println("**********Found records total count: " + lines2.count());
		System.out.println("Operation 1 [ENDS] : " + new Timestamp(System.currentTimeMillis()));
		
		/**
		 * TASK 1 finished
		 */
		
		/**
		 * Task 2: Filter all the records having RatecodeID as 4
		 */
		System.out.println("Operation 2 [STARTS] : " + new Timestamp(System.currentTimeMillis()));
		String header = lines.first(); //extract header
		//lines = lines.filter(row -> !row.equalsIgnoreCase(header));   //filter out header
		JavaRDD<String> filtered = lines.filter(line -> {
			if (!line.equalsIgnoreCase(header)) {
	            String[] list = line.split(",");
	            if(list.length >= 5 && Integer.parseInt(list[5]) == 4) {
	                return true;
	            }
	            return false;
			} else {
				return false;
			}
        });

        /*for(String line:filtered.collect()){
            System.out.println("filtered "+line);
        }*/
        
        System.out.println("**********Filtered total count: " + filtered.count());
        System.out.println("Operation 2 [ENDS] : " + new Timestamp(System.currentTimeMillis()));
        
		/**
		 * Task 3: Group By all the records based on payment type 
		 *         and find the count for each group. Sort the 
		 *         payment types in ascending order of their count.
		 */
        System.out.println("Operation 3 [STARTS] : " + new Timestamp(System.currentTimeMillis()));
        JavaRDD<String> filterRDD = lines.filter(line -> {
			if (!line.equalsIgnoreCase(header) || !line.isEmpty()) {
	            return true;
			} else {
				return false;
			}
        });
        JavaPairRDD<String, Integer> pairRdd = filterRDD.mapToPair( line -> {
        			String[] list = line.split(",");
        			if (list.length >=9 )
        				return new Tuple2<String, Integer>(list[9],1);
        			else 
        				return new Tuple2<String, Integer>("",1);
        });
        JavaPairRDD<String, Integer> countRdd = pairRdd.reduceByKey((x,y) -> x+y);
        countRdd.foreach(x -> System.out.println("Payment type: " + x._1 + ":" + x._2));
        System.out.println("Operation 3 [ENDS] : " + new Timestamp(System.currentTimeMillis()));
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex.getMessage());
		}		
        finally{
    		sc.close();
        }
	}
}
