import common.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.Operator;

public class HelperMethods {
  private static final Logger logger = LogManager.getLogger(HelperMethods.class);

  public static List<Tuple> collectAllTuples(Operator operator) {
    Tuple tuple;
    List<Tuple> tuples = new ArrayList<>();
    while ((tuple = operator.getNextTuple()) != null) {
      tuples.add(tuple);
    }

    return tuples;
  }

  public static List<Tuple> getExpectedTuples(Path dirPath, int index) {
    String inputFilePath = dirPath.toString() + "/query" + (index + 1);
    logger.info("Reading expected output from: " + inputFilePath);
    HumanReadableTupleReader reader = new HumanReadableTupleReader(inputFilePath);
    List<Tuple> tuples = reader.getAllTuples();
    reader.close();
    return tuples;
  }

  public static void writeOutput(Path dirPath, int index, List<Tuple> tuples) {
    String outputFilePath = dirPath.toString() + "/query" + (index + 1);
    if (tuples.isEmpty()) {
      return;
    }
    HumanReadableTupleWriter writer =
        new HumanReadableTupleWriter(outputFilePath, tuples.getFirst().getAllElements().size());

    logger.info("Writing output to: " + outputFilePath);
    writer.writeTuples(tuples);
    writer.close();
  }

  public static void generateBinaryInputFile(Path resourcePath) throws IOException {
    logger.info("Generating binary input files");

    String inputDir = resourcePath.resolve("data_human_readable").toString();
    String outputDir = resourcePath.resolve("data").toString();

    // Get the list of tables
    DBCatalog dbCatalog = DBCatalog.getInstance();
    List<String> tables = dbCatalog.getTables();

    for (String tableName : tables) {

      // Read the tuples from the file
      String inputFile = inputDir + "/" + tableName;
      HumanReadableTupleReader reader = new HumanReadableTupleReader(inputFile);
      List<Tuple> tuples = reader.getAllTuples();
      reader.close();

      // Write the tuples to a binary file
      String outputFile = outputDir + "/" + tableName;
      if (tuples.isEmpty()) {
        continue;
      }
      BinaryTupleWriter writer =
          new BinaryTupleWriter(outputFile, tuples.getFirst().getAllElements().size());
      writer.writeTuples(tuples);
      writer.close();
    }
  }
}
