import common.*;
import java.io.IOException;
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

  public static void generateBinaryInputFile(String resourcePath) throws IOException {
    ArrayList<String> tables = new ArrayList<>();
    tables.add("Boats");
    tables.add("Reserves");
    tables.add("Sailors");

    for (String tableName : tables) {
      int tupleSize = DBCatalog.getInstance().getSchemaForTable(tableName).size();

      // Read the tuples from the file
      String inputFilePatch = resourcePath + "/data_human_readable/" + tableName;
      logger.info("Reading from: " + inputFilePatch);
      HumanReadableTupleReader reader = new HumanReadableTupleReader(inputFilePatch, tupleSize);

      ArrayList<Tuple> tuples = new ArrayList<>();
      Tuple tuple;
      while ((tuple = reader.getNextTuple()) != null) {
        tuples.add(tuple);
      }
      reader.close();
      logger.info("Read " + tuples.size() + " tuples from " + inputFilePatch);

      // Write the tuples to a binary file
      String outputFilePath = resourcePath + "/data/" + tableName;
      logger.info("Writing to: " + outputFilePath);
      BinaryTupleWriter writer = new BinaryTupleWriter(outputFilePath, tupleSize);
      for (Tuple t : tuples) {
        writer.writeTuple(t);
      }
      writer.close();
      logger.info("Written " + tuples.size() + " tuples to " + outputFilePath);
    }
  }
}
