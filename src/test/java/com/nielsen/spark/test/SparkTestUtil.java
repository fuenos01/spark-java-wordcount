package com.nielsen.spark.test;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;

import java.io.File;

public class SparkTestUtil {
  private static final boolean IS_WINDOWS = SystemUtils.IS_OS_WINDOWS;

  public static void initTestEnv() {
    String user_dir = System.getProperty("user.dir").replace('\\', '/');

    String spark_hive_warehouse_dir = user_dir + "/spark-hive";

    if (IS_WINDOWS)
      spark_hive_warehouse_dir = "file:///" + spark_hive_warehouse_dir;

    String log4j_file = "file:///" + user_dir + "/conf/log4j.properties";

    System.setProperty("hive.exec.scratchdir", user_dir + "/tmp");
    System.setProperty("java.io.tmpdir", user_dir + "/tmp/spark/tmp");
    System.setProperty("hadoop.home.dir", user_dir + "/hadoop");
    System.setProperty("log4j.configuration", log4j_file);

    System.setProperty("spark.sql.warehouse.dir", spark_hive_warehouse_dir);
    System.setProperty("spark.local.dir", user_dir + "/tmp/spark/scratch");
    System.setProperty("spark.master", "local");
    //int cores = Runtime.getRuntime().availableProcessors();
    //System.setProperty("spark.master", "local[" + cores + "]");
    //System.setProperty("spark.sql.shuffle.partitions", "" + cores);

    initTmpFolder(user_dir);
  }

  private static void initTmpFolder(String user_dir) {
    try {
      String tmp_folder = user_dir + "/tmp";
      File file = new File(tmp_folder);
      if (file.exists())
        FileUtils.deleteDirectory(file);
      file.mkdir();

      String line = "chmod -R 777 " + tmp_folder;

      if (IS_WINDOWS)
        line = user_dir + "/hadoop/bin/winutils.exe " + line;

      CommandLine cmd_line = CommandLine.parse(line);
      DefaultExecutor executor = new DefaultExecutor();

      int exit_value = executor.execute(cmd_line);
      if (exit_value != 0)
        throw new Exception("Error while executing chmod on tmp folder => " + tmp_folder);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}