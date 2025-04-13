import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * HW3 implementation of spark streaming context
 */
public class HW3 {

    // After how many items we should stop
    public static final int THRESHOLD = 10000000;

    /**
     * Helper function that returns the median of an input array
     * @param arr input array of long values
     * @return median of the input array
     */
    public static long getMedian(long[] arr) {
        // sort the array in increasing order
        Arrays.sort(arr);

        // if the length of the array is even, then return the mean of the two middle values
        if(arr.length % 2 == 0)
            return (arr[(arr.length - 2) / 2] + arr[arr.length / 2]) / 2;
        else // if the length is odd, return the middle value
            return arr[arr.length/2];
    }

    /**
     * Function that calculates the estimated frequency of an item
     * @param a_h vector that contains the first values used to compute the h functions
     * @param b_h vector that contains the second values used to compute the h functions
     * @param a_g vector that contains the first values used to compute the g functions
     * @param b_g vector that contains the second values used to compute the g functions
     * @param p 8191
     * @param item item for which we want to calculate the estimated frequency
     * @param D number of rows of the count sketch matrix
     * @param W number of columns of the count sketch matrix
     * @param C count sketch matrix
     * @return the estimated frequency of the item
     */
    public static long estimatedFrequency(int[] a_h, int[] b_h, int[] a_g, int[] b_g, int p, long item, int D, int W, int[][] C) {
        long[] frequencies = new long[D];

        for(int i = 0; i < D; i++) {
            frequencies[i] = (long) C[i][h(a_h[i], b_h[i], p, (int) item, W)] * g(a_g[i], b_g[i], p, (int)item);
        }

        return getMedian(frequencies);
    }

    /**
     * Function that computes the column index associated with the hash function h based on an item
     * @param a hash function first value
     * @param b hash function second value
     * @param p 8191
     * @param item item for which to compute h(item)
     * @param W number of columns of the count sketch
     * @return random value calculated through the hash function
     */
    public static int h(int a, int b, int p, int item, int W) {
        return (int) (((long) a * item + b) % p) % W;
    }

    /**
     * Function that computes the value g(item)
     * @param a hash function first value
     * @param b hash function second value
     * @param p 8191
     * @param item item for which to compute g(item)
     * @return g(item)
     */
    public static int g(int a, int b, int p, int item) {
        return (((int) (((long) a * item + b) % p) % 2) == 1 ? 1 : -1);
    }

    public static void main(String[] args) throws Exception {
        // number of rows of the count sketch matrix
        final int D;
        // number of columns of the count sketch matrix
        final int W;
        // the left endpoint of the interval of interest
        final int left;
        // the right endpoint of the interval of interest
        final int right;
        // the number of top frequent items of interest
        final int K;
        // the port number
        final int portExp;

        /*
          Check if all the command line parameters are provided. What is needed is:
          - D: number of rows of the count sketch matrix;
          - W: number of columns of the count sketch matrix;
          - left: left endpoint of the interval of interest;
          - right: right endpoint of the interval of interest;
          - K: number of top frequent items in the stream;
          - portExp: port number;
         */
        if (args.length != 6) {
            throw new IllegalArgumentException("USAGE: D, W, left, right, K, portExp");
        }

        // IMPORTANT: when running locally, it is *fundamental* that the
        // `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("DistinctExample");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore.
        // The main thread will first acquire the only permit available and then try
        // to acquire another one right after spinning up the streaming computation.
        // The second tentative at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met we release the semaphore, basically giving "green light" to the main
        // thread to shut down the computation.
        // We cannot call `sc.stop()` directly in `foreachRDD` because it might lead
        // to deadlocks.
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        D = Integer.parseInt(args[0]);
        W = Integer.parseInt(args[1]);
        left = Integer.parseInt(args[2]);
        right = Integer.parseInt(args[3]);
        K = Integer.parseInt(args[4]);
        portExp = Integer.parseInt(args[5]);

        System.out.println("Receiving data from port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements
        final int p = 8191;

        //Count sketch matrix
        int[][] C = new int[D][W];

        int[] a_h = new int[D];
        int[] b_h = new int[D];

        int[] a_g = new int[D];
        int[] b_g = new int[D];

        Random random = new Random();

        //Define the random values to use to compute the hash functions
        for(int i = 0; i < D; i++) {
            a_h[i] = 1 + random.nextInt(p - 1);
            b_h[i] = random.nextInt(p);

            a_g[i] = 1 + random.nextInt(p - 1);
            b_g[i] = random.nextInt(p);
        }

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < THRESHOLD) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;

                        // Extract the distinct items from the batch
                        Map<Long, Long> batchItems = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .reduceByKey((i1, i2) -> i1 + i2) //sum all true frequencies of each distinct item
                                .collectAsMap();

                        // Iterate through all the distinct items read in the current batch
                        for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                            // If the value is within the interval add it to the histogram
                            if((pair.getKey() >= left && pair.getKey() <= right)) {
                                // If not present, add it with the associated true frequency
                                if (!histogram.containsKey(pair.getKey())) {
                                    histogram.put(pair.getKey(), pair.getValue());
                                }
                                else { // Sum the current true frequency of the item with the one of the current batch
                                    histogram.put(pair.getKey(), histogram.get(pair.getKey()) + pair.getValue());
                                }
                            }
                        }

                        // Update the count sketch matrix based on the items in the current batch
                        for(String elem : batch.collect()) {
                            int value = Integer.parseInt(elem);

                            //If the value is within the interval add it to the histogram
                            if(value >= left && value <= right) {
                                for(int i = 0; i < D; i++) {
                                    // Add or subtract 1 to the cell corresponding to row i and column h(elem)
                                    // depending on the value of the function g(elem)
                                    C[i][h(a_h[i], b_h[i], p, value, W)] += g(a_g[i], b_g[i], p, value);
                                }
                            }
                        }

                        /*
                            if (batchSize > 0) {
                                System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                            }
                         */

                        // Check if we have read at least THRESHOLD items in the stream
                        if (streamLength[0] >= THRESHOLD) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");

        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
        System.out.println("Streaming engine stopped");

        // compute the number of total items read from the stream within the interval R
        long rCount = histogram.values().stream().mapToLong(Long::longValue).sum();

        // Output of input parameters provided as command-line arguments
        System.out.println("D = " + D + ", W = " + W + " [left,right] = [" + left + ", " + right + "], K = "+K+", Port = "+portExp);

        // Output lengths of the streams |Σ|
        System.out.println("Total number of items = " + streamLength[0]);

        // Output lengths of the streams |Σ_R|
        System.out.println("Total number of items in [" + left + ", " + right + "] = " + rCount);

        // Output of the number of distinct items in Σ_R
        System.out.println("Total number of items in [" + left + ", " + right + "] = " + histogram.keySet().size());

        // Convert the HashMap to a List of Map.Entry objects
        List<Map.Entry<Long, Long>> entryList = new ArrayList<>(histogram.entrySet());

        // Sort the list based on the frequency in decreasing order
        Collections.sort(entryList, new Comparator<Map.Entry<Long, Long>>() {
            @Override
            public int compare(Map.Entry<Long, Long> entry1, Map.Entry<Long, Long> entry2) {
                return entry2.getValue().compareTo(entry1.getValue());
            }
        });

        // Create a new LinkedHashMap to preserve the order of the sorted entries
        LinkedHashMap<Long, Long> sortedMap = new LinkedHashMap<>();

        // Iterate over the sorted list and add the entries to the new LinkedHashMap
        for (Map.Entry<Long, Long> entry : entryList) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        // helper variables to compute the second moment and the error
        long F2 = 0;
        long F2_estimated = 0;
        int i = 0;
        double error = 0;
        int count = 0;

        // true frequency of the k-th most frequent element
        long kFreq = sortedMap.get(sortedMap.keySet().toArray()[K-1]);

        // iterate through all the pairs ordered based on their true frequency
        for(Map.Entry<Long, Long> entry : sortedMap.entrySet()) {
            //Update the second moment and current error
            F2 += Math.pow(entry.getValue(), 2);
            long freq = estimatedFrequency(a_h, b_h, a_g, b_g, p, entry.getKey(), D, W, C);

            // Output the true and estimated frequencies of the top K frequent items
            if(K <= 20 && i < K) {
                System.out.println("Item "+entry.getKey() + " frequency = "+entry.getValue() + ", estimated frequency = "+freq);
                i++;
            }

            // compute the error of the top K frequent items (also considering the ones with equal true frequency)
            if(entry.getValue() >= kFreq) {
                error += Math.abs((double) entry.getValue() - freq) / entry.getValue();
                count++;
            }
        }

        // Output of the average relative error of the top K frequent items
        System.out.println("Avg err for top "+K+" = "+error / count);

        long [] secondMomentEstimated = new long[D];

        // compute the estimated frequency for all the distinct items in the interval [left, right]
        for(int j = 0; j < D; j++) {
            for (int k = 0; k < W; k++) {
                secondMomentEstimated[j] += Math.pow(C[j][k], 2);
            }
        }

        F2_estimated += getMedian(secondMomentEstimated);

        // Output of F2 and F2 estimate
        System.out.println("F2 = " + F2/(Math.pow(rCount, 2)));
        System.out.println("F2 estimated = " + F2_estimated/(Math.pow(rCount, 2)));
    }
}
