package com.trainologic.spark.course.rdd;

import com.trainologic.spark.course.utils.PU;
import com.trainologic.spark.course.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.util.*;

public class Broadcasting {
    private static List<Integer> createRange(int num) {
        List<Integer> l = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            l.add(i);
        }
        return l;
    }

    public static void broadcasting1() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        Set<Integer> ignored = new HashSet<>(Arrays.asList(1, 3, 10));

        JavaRDD<Integer> intRDD = sc.parallelize(createRange(20));

        //Problmatic!
        JavaRDD<Integer> res = intRDD.filter(in -> !ignored.contains(in));

        PU.print("Filtered", res);
        sc.close();
    }

    public static void broadcasting2() {
        JavaSparkContext sc = SparkUtils.createSparkContext();
        Set<Integer> ignored = new HashSet<>(Arrays.asList(1, 3, 10));

        Broadcast<Set<Integer>> ignoredBroadcast = sc.broadcast(ignored);

        JavaRDD<Integer> intRDD = sc.parallelize(createRange(20));

        JavaRDD<Integer> res = intRDD.filter(in -> !ignoredBroadcast.getValue().contains(in));

        PU.print("Filtered", res);
        sc.close();
    }

    public static void accum1() {
        JavaSparkContext sc = SparkUtils.createSparkContext();

        JavaRDD<Integer> intRDD = sc.parallelize(createRange(20));
        Set<Integer> ignored = new HashSet<>(Arrays.asList(1, 3, 10));

        LongAccumulator yes = sc.sc().longAccumulator("yes");
        LongAccumulator no = sc.sc().longAccumulator("no");

        JavaRDD<Integer> res = intRDD.map(in -> {
            if (ignored.contains(in)) {
                yes.add(1);
            } else {
                no.add(1);
            }
            return in;
        });

        res.count();
        System.out.println(yes.count());
        System.out.println(no.count());
        System.out.println(yes.avg());
        System.out.println(yes.sum());
        sc.close();
    }

    public static void main(String[] args) throws IOException {
//        broadcasting2();
        accum1();

    }
}
