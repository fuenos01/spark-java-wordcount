package com.nielsen.spark.examples.wordcount;

import com.nielsen.spark.test.SparkTestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

public class MainTest {
  @Test
  public void testMain() throws Exception {
    String[] args = new String[]{"src/test/resources/data/", "tmp/output/"};
    Main.main(args);
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    SparkTestUtil.initTestEnv();
  }

}