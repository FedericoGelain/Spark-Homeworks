import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.*;

/**
 * HW1 implementation of triangle counting using mapReduce
 */
public class HW1 {
    /**
     * The provided method that computes the number of triangles in the input graph
     * @param edgeSet set of edges that compose the graph
     * @return the number of triangles in the input graph
     */
    public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {uAdj = new HashMap<>();}
            uAdj.put(v,true);
            adjacencyLists.put(u,uAdj);
            if (vAdj == null) {vAdj = new HashMap<>();}
            vAdj.put(u,true);
            adjacencyLists.put(v,vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v>u) {
                    HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w>v && (uAdj.get(w)!=null)) numTriangles++;
                    }
                }
            }
        }
        return numTriangles;
    }

    /**
     * Method that implements the algorithm 1
     * @param edges list of edges
     * @param C number of colors
     * @return the number of triangles contained in the input edges
     */
    public static Long MR_ApproxTCwithNodeColors(JavaPairRDD<Integer, Integer> edges, int C) {
        // compute the random values which will be used to color the nodes (same values for this run of the algorithm)
        Random random = new Random();
        final int p = 8191;
        int a = 1 + random.nextInt(p-1);
        int b = random.nextInt(p);

        // MAP PHASE (R1) - assign color to each vertex and create tuple with same color vertices
        JavaPairRDD<Integer,Long> tfinal = edges.flatMapToPair((tuple) -> {
            ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();

            // calculate the color for each vertex that composed an edge
            int hC1 = ((a * tuple._1() + b) % p ) % C;
            int hC2 = ((a * tuple._2() + b) % p ) % C;

            // if the nodes have the same color, you keep them
            if(hC1 == hC2)
                pairs.add(new Tuple2<>(hC1, new Tuple2<>(tuple._1(), tuple._2())));

            return pairs.iterator();
        })

        // SHUFFLE + GROUPING
        .groupByKey()

        // REDUCE PHASE (R1) - partition based on the color of each edge
        .flatMapToPair((tuple) -> {
            // arraylist that will contain all pairs of vertices with same color
            ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

            // arraylist that will contain the pairs (color, number of monochromatic triangles)
            ArrayList<Tuple2<Integer, Long>> countTriangles = new ArrayList<>();

            // add to an arraylist all vertices with same color since CountTriangles requires an ArrayList of edges,
            // and we currently have an Iterable of them
            tuple._2().forEach(pairs::add);

            // add to countTriangles the number of triangles of each partitions
            countTriangles.add(new Tuple2<>(0, CountTriangles(pairs)));

            return countTriangles.iterator();
        })

        // MAP PHASE (R2) - empty (we simply propagate the pairs obtained at the end of R1)

        // REDUCE PHASE (R2) - compute the total number of triangles
        .reduceByKey((x, y) -> x+y);

        return C * C * tfinal.values().first();
    }

    /**
     * Method that implements the algorithm 2
     * @param edges list of edges
     * @return the number of triangles contained in the input edges
     */
    public static Long MR_ApproxTCwithSparkPartitions(JavaPairRDD<Integer, Integer> edges) {
        // MAP PHASE (R1) - assign a random color to each edge and create tuple with same color vertices
        // (already done using the method repartitions(C))

        // REDUCE PHASE (R1) - compute the number of triangles for each partition
        JavaPairRDD<Integer, Long> tfinal = edges.mapPartitionsToPair(elements -> {
            // arraylist that will contain all edges with same color
            ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

            // arraylist that will contain the pairs (color, number of monochromatic triangles)
            ArrayList<Tuple2<Integer, Long>> countTriangles = new ArrayList<>();

            // adding all edges with same color to an arraylist
            while(elements.hasNext())
                pairs.add(elements.next());

            // add to countTriangles all numbers of triangles of each partitions
            countTriangles.add(new Tuple2<>(0, CountTriangles(pairs)));
            return countTriangles.iterator();
        })

        // MAP PHASE (R2) - empty (we simply propagate the pairs obtained at the end of R1)

        // REDUCE PHASE (R2) - compute the total number of triangles
        .reduceByKey((x, y) -> x+y);

        return edges.getNumPartitions() * edges.getNumPartitions() * tfinal.values().first();
    }

    /**
     * Helper function that returns the median of an input array
     * @param arr input array of long values
     * @return median of the input array
     */
    public static long getMedian(long[] arr) {
        //sort the array in increasing order
        Arrays.sort(arr);

        //if the length of the array is even, then return the mean of the two middle values
        if(arr.length % 2 == 0)
            return (arr[(arr.length - 2) / 2] + arr[arr.length / 2]) / 2;
        else //if the length is odd, return the middle value
            return arr[arr.length/2];
    }

    public static void main(String[] args) {

        // number of colors
        final int C;

        // number of repetitions
        final int R;

        /*
          Check if all the command line parameters are provided. What is needed is:
          - C: number of colors;
          - R: number of runs of the algorithm MR_ApproxTCwithNodeColors;
          - file_path: path of the input file.
         */
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: C R file_path");
        }

        // spark setup
        SparkConf conf = new SparkConf(true).setAppName("TriangleCounting");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // read parameters C and R
        C = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);

        //if the number of colors and/or the number of rounds is 0 or negative, then the execution stops
        if(C <= 0 || R <= 0) {
            System.out.println("C and R must be positive values");
            return ;
        }

        //since textFile() doesn't throw any exception if the file path provided isn't valid, not even IOException
        //(the only similar thing could be InvalidInputException when you invoke .count() with any JavaRDD created
        //starting from the invalid file), we do the check here.
        if (!(new File(args[2]).isFile())) {
            System.out.println("Invalid input path");
            return ;
        }

        /*
         read input graph as RDD of strings. To avoid undesired default repartitioning,
         we already partition the RDD in C partitions (by default it uses at minimum 2 partitions, which
         is something we'd like to avoid in the case in which C = 1)
        */
        JavaRDD<String> rawData = sc.textFile(args[2]).repartition(C);

        // transform RDD of string into an RDD of pairs of integer
        JavaPairRDD<Integer, Integer> edges = rawData.flatMapToPair((document) -> {
            String[] tokens = document.split("\n");

            ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

            for (String token : tokens) {
                String[] nodes = token.split(",");
                pairs.add(new Tuple2<>(Integer.parseInt(nodes[0]),Integer.parseInt(nodes[1])));
            }

            return pairs.iterator();
        }).cache(); // cached the RDD

        // print the input parameters + the number of edges of the graph
        System.out.println("Dataset = " + args[2] + "\nNumber of Edges = " + edges.count() + "\nNumber of Colors = " +
                C + "\nNumber of Repetitions = " + R);

        // runs R times MR_ApproxTCwithNodeColors
        long[] total_counts = new long[R];

        // useful variables to measure the run time of algorithms
        long startTime = 0, endTime = 0, runTime = 0;
        long totalTime = 0;

        System.out.println("*** Approximation through node coloring ***");

        for(int i = 0; i < R; i++) {
            startTime = System.nanoTime();

            total_counts[i] = MR_ApproxTCwithNodeColors(edges, C);

            endTime   = System.nanoTime();
            runTime = endTime - startTime;
            totalTime += runTime;
        }

        System.out.println("Number of triangles (median over " + R + " runs): " + getMedian(total_counts));

        System.out.println("Running time (average over " + R + " runs): " + (totalTime / R)/ 1000000 + " ms");

        System.out.println("*** Approximation through Spark partitions ***");

        startTime = System.nanoTime();

        System.out.println("Number of triangles: " + MR_ApproxTCwithSparkPartitions(edges));

        endTime   = System.nanoTime();
        totalTime = endTime - startTime;

        System.out.println("Running time: "+ (totalTime  / R) / 1000000 + " ms");
    }
}
