package index;

import config.DBCatalog;
import config.IndexConfigManager;
import config.IndexDefinition;
import io.reader.BinaryTupleReader;
import io.reader.TupleReader;
import io.writer.BinaryTupleWriter;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.TupleComparator;

public class IndexBuilder {
  private static final Logger logger = LogManager.getLogger(IndexBuilder.class);
  private final IndexConfigManager indexConfigManager;
  private final DBCatalog dbCatalog;

  public IndexBuilder() {
    this.indexConfigManager = IndexConfigManager.getInstance();
    this.dbCatalog = DBCatalog.getInstance();
  }

  public void buildIndexes() {
    for (IndexDefinition indexDefinition : indexConfigManager.getIndexConfigs()) {
      try {
        buildIndex(indexDefinition);
      } catch (Exception e) {
        logger.error(
            "Failed to build index for relation {} attribute {}: {}",
            indexDefinition.getRelation(),
            indexDefinition.getAttribute(),
            e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }

  void buildIndex(IndexDefinition indexDefinition) throws IOException {
    String relation = indexDefinition.getRelation();
    String attribute = indexDefinition.getAttribute();
    int attributeIndex = indexDefinition.getAttributeIndex();
    int order = indexDefinition.getOrder();

    logger.info("Building index on relation {} attribute {} order {}", relation, attribute, order);

    String relationPath = dbCatalog.getTablePath(relation);

    // Handle clustered index - sort the relation file
    if (indexDefinition.isClustered()) {
      sortAndReplaceTable(relationPath, attributeIndex);
    }

    List<Integer> keys = new ArrayList<>();
    List<RecordId> recordIds = new ArrayList<>();

    // loop through the relation file and populate keys and recordIds
    readKeyAndRecordIds(relationPath, keys, attributeIndex, recordIds);

    // create the index file for the relation
    try (FileChannel fileChannel =
        new FileOutputStream(indexDefinition.getIndexFilePath()).getChannel()) {
      BPlusTree.buildAndSerializeBPlusTree(fileChannel, keys, recordIds, order);
    }
  }

  private static void readKeyAndRecordIds(
      String relationPath, List<Integer> keys, int attributeIndex, List<RecordId> recordIds)
      throws IOException {
    try (FileChannel fileChannel = new FileInputStream(relationPath).getChannel()) {

      BinaryTupleReader tupleReader = new BinaryTupleReader(fileChannel);

      RecordId recordId =
          new RecordId(tupleReader.getCurrentPageNumber(), tupleReader.getCurrentTupleNumber());
      Tuple tuple = tupleReader.getNextTuple();

      while (tuple != null) {
        keys.add(tuple.getElementAtIndex(attributeIndex));
        recordIds.add(recordId);

        // get the next tuple
        recordId =
            new RecordId(tupleReader.getCurrentPageNumber(), tupleReader.getCurrentTupleNumber());
        tuple = tupleReader.getNextTuple();
      }
    }
  }

  private static void sortAndReplaceTable(String relationPath, int attributeIndex)
      throws IOException {
    List<Tuple> tuples;

    // read all tuples from the relation file
    try (FileChannel fileChannel = new FileInputStream(relationPath).getChannel()) {
      TupleReader tupleReader = new BinaryTupleReader(fileChannel);
      tuples = tupleReader.getAllTuples();
    }

    // sort the tuples based on the attribute
    tuples.sort(new TupleComparator(List.of(attributeIndex)));

    // write the sorted tuples back to the relation file
    try (BinaryTupleWriter tupleWriter =
        new BinaryTupleWriter(
            new FileOutputStream(relationPath).getChannel(),
            tuples.getFirst().getAllElements().size())) {
      tupleWriter.writeTuples(tuples);
    }
  }
}
