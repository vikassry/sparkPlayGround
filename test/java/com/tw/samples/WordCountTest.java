package com.tw.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordCountTest implements Serializable {

    protected transient JavaSparkContext javaSparkContext;

    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster("local");
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    @After
    public void tearDown() throws Exception {
        javaSparkContext.close();
    }

    @Test
    public void shouldBeAbleToDoWordCountWithSimpleStrings() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("This is a text", "Another text", "More text", "a text"));

        JavaPairRDD<String, Integer> wordCount = JavaWordCount.wordCount(initialDataset);

        List<Tuple2<String, Integer>> result = wordCount.collect();

        List<Tuple2<String, Integer>> expected = new ArrayList<Tuple2<String, Integer>>();
        expected.add(new Tuple2<String, Integer>("Another", 1));
        expected.add(new Tuple2<String, Integer>("is", 1));
        expected.add(new Tuple2<String, Integer>("a", 2));
        expected.add(new Tuple2<String, Integer>("text", 4));
        expected.add(new Tuple2<String, Integer>("This", 1));
        expected.add(new Tuple2<String, Integer>("More", 1));

        Assert.assertEquals(expected, result);
    }
}
