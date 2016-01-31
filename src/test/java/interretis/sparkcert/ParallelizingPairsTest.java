package interretis.sparkcert;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParallelizingPairsTest {

    private static JavaSparkContext sc;

    @BeforeClass
    public static void setUp() {
        final SparkConf conf = new SparkConf();
        conf.setAppName("test application");
        conf.setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void tearDown() {
        if (sc != null) {
            sc.close();
        }
    }

    @Test
    public void parallelizing_pairs() {
        // given
        final List<Tuple2<String, Integer>> tuples = Lists.newArrayList(
                new Tuple2<>("one", 1),
                new Tuple2<>("two", 2),
                new Tuple2<>("three", 3),
                new Tuple2<>("four", 4)
        );
        // when
        final JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(tuples);
        // then
        assertEquals(rdd.count(), 4);
    }
}
