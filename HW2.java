import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 * HW2 implementation of triangle counting using mapReduce
 */
public class HW2 {
    /**
     * The provided method that computes the number of triangles in the input graph
     * @param edgeSet set of edges that composes the graph
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
     * The provided method that computes the exact number of triangles in the input graph
     * @param edgeSet set of edges that composes the graph
     * @param key the triplet of integers containing the three colors
     * @param a a random number between [1, p - 1]
     * @param b a random number between [0, p - 1]
     * @param p a prime number equal to 8191
     * @param C number of colors
     * @return the exact number of triangles in the input graph
     */
    public static Long CountTriangles2(ArrayList<Tuple2<Integer, Integer>> edgeSet, Tuple3<Integer, Integer, Integer> key, long a, long b, long p, int C) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        HashMap<Integer, Integer> vertexColors = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            if (vertexColors.get(u) == null) {vertexColors.put(u, (int) ((a*u+b)%p)%C);}
            if (vertexColors.get(v) == null) {vertexColors.put(v, (int) ((a*v+b)%p)%C);}
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
                        if (w>v && (uAdj.get(w)!=null)) {
                            ArrayList<Integer> tcol = new ArrayList<>();
                            tcol.add(vertexColors.get(u));
                            tcol.add(vertexColors.get(v));
                            tcol.add(vertexColors.get(w));
                            Collections.sort(tcol);
                            boolean condition = (tcol.get(0).equals(key._1())) && (tcol.get(1).equals(key._2())) && (tcol.get(2).equals(key._3()));
                            if (condition) {numTriangles++;}
                        }
                    }
                }
            }
        }
        return numTriangles;
    }

    /**
     * Method that implements algorithm 1
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
     * Method that implements algorithm 2
     * @param edges list of all the edges of the graph
     * @param C number of colors
     * @return the exact number of triangles of the graph
     */
    public static long MR_ExactTC(JavaPairRDD<Integer, Integer> edges, int C){
        // compute the random values which will be used to color the nodes (same values for this run of the algorithm)
        Random random = new Random();
        final int p = 8191;
        int a = 1 + random.nextInt(p-1);
        int b = random.nextInt(p);

        // MAP PHASE (R1) - assign a color to each vertex and create
        JavaPairRDD<Integer,Long> tfinal = edges.flatMapToPair((tuple) -> {
                    ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();

                    // calculate the color for each vertex
                    // - Since the calculations (a*u)+b can result in a value which is bigger than what an integer can store
                    // (for the largest files) we cast it to a long in order to avoid overflow problems. -
                    int hC1 = (int)(((long) a * tuple._1() + b) % p ) % C;
                    int hC2 = (int)(((long) a * tuple._2() + b) % p ) % C;

                    // create the key for each vertex (triplet consisting of the colors of the vertices forming the edge
                    // plus each possible color)
                    for(int i = 0; i < C; i++) {
                        int min = Math.min(hC1, Math.min(hC2, i));
                        int max = Math.max(hC1, Math.max(hC2, i));
                        int mid = hC1 + hC2 + i - max - min;

                        // key defined with the three colors in non-decreasing order
                        Tuple3<Integer, Integer, Integer> key = new Tuple3<>(min, mid, max);

                        pairs.add(new Tuple2<>(key, new Tuple2<>(tuple._1(), tuple._2())));
                    }
                    return pairs.iterator();
                })

                // SHUFFLE + GROUPING
                .groupByKey()

                // REDUCE PHASE (R1) - partition based on each key
                .flatMapToPair((tuple) -> {
                    // arraylist that will contain all possible pairs of vertices that have same key
                    ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

                    // arraylist that will contain the pairs (0, number of triangles)
                    ArrayList<Tuple2<Integer, Long>> countTriangles = new ArrayList<>();

                    // add to an arraylist all edges since CountTriangles requires an ArrayList of edges,
                    // and we currently have an Iterable of them
                    tuple._2().forEach(pairs::add);

                    // add to countTriangles the number of triangles of each partitions
                    countTriangles.add(new Tuple2<>(0, CountTriangles2(pairs, tuple._1(), a, b, p, C)));

                    return countTriangles.iterator();
                })

                // MAP PHASE (R2) - empty (we simply propagate the pairs obtained at the end of R1)

                // REDUCE PHASE (R2) - compute the total number of triangles
                .reduceByKey((x, y) -> x+y);

        return tfinal.values().first();
    }

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
     *
     * @param args command line arguments. If provided, args[0] contains the number of colors, args[1] contains the
     *             number of repetitions, args[2] contains the flag to decide which algorithm will be run and args[3]
     *             contains the path of the input file.
     */
    public static void main(String[] args) {

        // number of colors
        final int C;

        // number of repetitions
        final int R;

        // the flag to decide which algorithm will be run
        final int F;

        /*
          Check if all the command line parameters are provided. What is needed is:
          - C: number of colors;
          - R: number of runs;
          - F: flag to decide which algorithm will be run;
          - file_path: path of the input file;
         */
        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: C R F file_path");
        }

        // spark setup
        SparkConf conf = new SparkConf(true).setAppName("TriangleCounting");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // force Spark to use all required executors even for small datasets
        conf.set("spark.locality.wait", "0s");
        sc.setLogLevel("WARN");

        // read parameters C, R and F
        C = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);
        F = Integer.parseInt(args[2]);

        // if the number of colors and/or the number of rounds is 0 or negative and/or F is different from 0 or 1, then
        // the execution stops
        if(C <= 0 || R <= 0 || !(F == 0 || F == 1)) {
            System.out.println("C and R must be positive values. F must be a flag (either 0 or 1)");
            return ;
        }

        /*
         Reads input graph as RDD of strings. To avoid undesired default repartitioning,
         we already partition the RDD in 32 partitions.
        */
        JavaRDD<String> rawData = sc.textFile(args[3]).repartition(32);

        // transform RDD of strings into an RDD of pairs of integer
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
        System.out.println("Dataset = " + args[3] + "\nNumber of Edges = " + edges.count() + "\nNumber of Colors = " +
                C + "\nNumber of Repetitions = " + R);

        // array that contains the running time for each run
        long[] total_counts = new long[R];

        // useful variables to measure the run time of algorithms
        long startTime = 0, endTime = 0, runTime = 0;
        long totalTime = 0;

        if(F == 0) {
            System.out.println("*** Approximation through node coloring ***");

            for (int i = 0; i < R; i++) {
                startTime = System.nanoTime();

                total_counts[i] = MR_ApproxTCwithNodeColors(edges, C);

                endTime = System.nanoTime();
                runTime = endTime - startTime;
                totalTime += runTime;
            }

            System.out.println("Number of triangles (median over " + R + " runs): " + getMedian(total_counts));

            System.out.println("Running time (average over " + R + " runs): " + (totalTime / R) / 1000000 + " ms");
        } else {
            System.out.println("*** Exact algorithm with node coloring ***");

            long count = 0;

            for(int i = 0; i < R; i++) {
                startTime = System.nanoTime();

                count = MR_ExactTC(edges, C);

                endTime   = System.nanoTime();
                runTime = endTime - startTime;
                totalTime += runTime;
            }

            System.out.println("Number of triangles: " + count);

            System.out.println("Running time (average over " + R + " runs): " + (totalTime / R)/ 1000000 + " ms");
        }
    }
}