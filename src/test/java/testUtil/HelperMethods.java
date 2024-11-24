package testUtil;

import io.reader.BinaryTupleReader;
import io.reader.HumanReadableTupleReader;
import io.writer.BinaryTupleWriter;
import io.writer.HumanReadableTupleWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.base.PhysicalOperator;

public class HelperMethods {

  static Logger logger = LogManager.getLogger(HelperMethods.class);

  public static List<Tuple> collectAllTuples(PhysicalOperator operator) {
    List<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = operator.getNextTuple()) != null) {
      tuples.add(tuple);
    }
    return tuples;
  }

  public static boolean compareTupleListsExact(List<Tuple> expected, List<Tuple> actual) {
    if (expected.size() != actual.size()) {
      return false;
    }

    for (int i = 0; i < expected.size(); i++) {
      if (!expected.get(i).equals(actual.get(i))) {
        return true;
      }
    }
    return true;
  }

  public static boolean compareTupleListsAnyOrder(List<Tuple> expected, List<Tuple> actual) {
    if (expected.size() != actual.size()) {
      return false;
    }

    HashSet<Tuple> expectedSet = new HashSet<>(expected);
    HashSet<Tuple> actualSet = new HashSet<>(actual);

    if (!expectedSet.equals(actualSet)) {
      return false;
    }
    return true;
  }

  public static void convertToBinaryFiles(String sourceDir, String destDir) throws IOException {
    logger.info("Converting human readable files to binary files");
    File humanReadableDir = new File(sourceDir);
    File binaryDataDir = new File(destDir);
    binaryDataDir.mkdirs();

    File[] files = humanReadableDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          List<Tuple> tuples = readHumanReadable(file.getPath());
          writeBinary(new File(binaryDataDir, file.getName()).getPath(), tuples);
        }
      }
    }
  }

  public static List<Tuple> readBinary(String filePath) throws IOException {
    BinaryTupleReader reader = new BinaryTupleReader(filePath);
    List<Tuple> tuples = reader.getAllTuples();
    reader.close();
    return tuples;
  }

  public static List<Tuple> readHumanReadable(String filePath) throws IOException {
    HumanReadableTupleReader reader = new HumanReadableTupleReader(filePath);
    List<Tuple> tuples = reader.getAllTuples();
    reader.close();
    return tuples;
  }

  public static void writeBinary(String filePath, List<Tuple> tuples) throws IOException {
    if (!tuples.isEmpty()) {
      BinaryTupleWriter writer =
          new BinaryTupleWriter(filePath, tuples.getFirst().getAllElements().size());
      writer.writeTuples(tuples);
      writer.close();
    }
  }

  public static void writeHumanReadable(String filePath, List<Tuple> tuples) throws IOException {
    if (!tuples.isEmpty()) {
      HumanReadableTupleWriter writer = new HumanReadableTupleWriter(filePath);
      writer.writeTuples(tuples);
      writer.close();
    }
  }
}
