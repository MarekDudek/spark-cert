package interretis.sparkcert;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PassingFunctionTest {

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
    public void anonymous_inner_class_but_not_really() {
        final JavaRDD<String> lines = sc.parallelize(Arrays.asList("debug", "info", "error"));
        final JavaRDD<String> errors = lines.filter(CONTAINS_ERROR);
        final List<String> result = errors.collect();
        assertEquals(result.size(), 1);
    }

    private static final Function<String, Boolean> CONTAINS_ERROR = new Function<String, Boolean>() {
        public Boolean call(String x) {
            return x.contains("error");
        }
    };

    @Test
    public void named_class() {
        final JavaRDD<String> lines = sc.parallelize(Arrays.asList("debug", "info", "error"));
        final JavaRDD<String> errors = lines.filter(new ContainsError());
        final List<String> result = errors.collect();
        assertEquals(result.size(), 1);
    }

    private static class ContainsError implements Function<String, Boolean> {
        @Override
        public Boolean call(String x) throws Exception {
            return x.contains("error");
        }
    }
}
