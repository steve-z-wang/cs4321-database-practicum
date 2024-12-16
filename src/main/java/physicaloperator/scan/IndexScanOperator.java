package physicaloperator.scan;

import catalog.DBCatalog;
import config.IndexConfigManager;
import config.IndexDefinition;
import index.BPlusTree;
import index.BPlusTreeLeafNode;
import index.RecordEntry;
import index.RecordId;
import io.reader.BinaryTupleReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.function.Supplier;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.PhysicalOperator;
import utils.DBConstants;

public class IndexScanOperator extends PhysicalOperator {

  private static final Logger logger = LogManager.getLogger(IndexScanOperator.class);

  private Integer highKey;
  private final Supplier<Tuple> selectedGetNextTuple;
  private final Supplier<Void> selectedReset;

  private FileChannel indexChannel;
  private ByteBuffer indexPageBuffer;
  private BPlusTree bPlusTree;

  // for index reset
  private int startLeafAddress;
  private int startEntryIndex;

  // for index traversal
  private BPlusTreeLeafNode currentleafNode;
  private int currentEntryIndex;
  private int currentRecorIndex;

  // for checking the range
  private int attributeIndex;

  // for table scan
  private FileChannel tableChannel; // table file channel
  private BinaryTupleReader tupleReader; // tuple reader for table
  private Tuple nextTuple;

  /**
   * The scan operator will return only the tuple with attribute values that are within the range of
   * lowKey and highKey (lowKey <= value <= highKey)
   */
  //  basic information

  private Table table;

  private String coloum;
  private Integer lowKey;

  public IndexScanOperator(
      Table table, IndexDefinition indexDefinition, Integer lowKey, Integer highKey) {
    super(null);

    logger.info("Creating IndexScanOperator for table: {}", table.getName());

    // create output schema with table reference
    initializeSchema(table);

    this.table = table;
    this.coloum = indexDefinition.getAttribute();
    this.lowKey = lowKey;
    this.highKey = highKey;
    this.attributeIndex = indexDefinition.getAttributeIndex();

    // create tuple reader for table
    initializeTupleReader(table);

    // create index reader
    initializeIndexReader(indexDefinition);

    initializeIndexStart(indexDefinition, lowKey);

    if (startLeafAddress == -1) {
      nextTuple = null;
    } else {
      try {
        currentEntryIndex = startEntryIndex;
        currentRecorIndex = 0;
        positionReaderAtRecord();
        nextTuple = getNextValidTuple();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // select the appropriate getNextTuple and reset methods
    if (indexDefinition.isClustered()) {
      selectedGetNextTuple = this::getNextTupleWithClusteredIndex;
      selectedReset = this::resetWithClusteredIndex;
    } else {
      selectedGetNextTuple = this::getNextTupleWithNonClusteredIndex;
      selectedReset = this::resetWithNonClusteredIndex;
    }
  }

  public String toString() {
    return "IndexScan[" + table.getName() + "," + coloum + "," + lowKey + "," + highKey + "]";
  }

  private void initializeSchema(Table table) {
    this.outputSchema = new ArrayList<>();
    for (Column dbSchemaColumn : DBCatalog.getInstance().getSchemaForTable(table.getName())) {
      this.outputSchema.add(new Column(table, dbSchemaColumn.getColumnName()));
    }
  }

  private boolean initializeIndexStart(IndexDefinition indexDefinition, Integer lowKey) {
    try {

      // init index traversal
      if (lowKey == null) {
        currentleafNode = bPlusTree.getFirstLeafNode(indexChannel, indexPageBuffer);
        startEntryIndex = 0;
      } else {
        currentleafNode = bPlusTree.findLeafNodeByKey(indexChannel, indexPageBuffer, lowKey);
        startEntryIndex = currentleafNode.findEntryIndexByKey(lowKey);

        // if we run out of entries in the current leaf node, move to the next leaf node & reset
        // entry
        if (startEntryIndex >= currentleafNode.getNumOfKeys()) {
          currentleafNode =
              bPlusTree.getNextLeafNode(indexChannel, indexPageBuffer, currentleafNode);

          // if we run out of leaf nodes, we have no valid start index
          if (currentleafNode == null) {
            startLeafAddress = -1;
            return false;
          }
          startEntryIndex = 0;
        }
      }
      startLeafAddress = currentleafNode.getAddress();
      return true;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void initializeIndexReader(IndexDefinition indexDefinition) {
    try {
      indexPageBuffer = ByteBuffer.allocate(DBConstants.INDEX_PAGE_SIZE);

      // get index file
      String indexPath = IndexConfigManager.getInstance().getIndexFilePath(indexDefinition);
      indexChannel = new FileInputStream(indexPath).getChannel();

      // deserialize the tree
      bPlusTree = BPlusTree.deserialize(indexChannel, indexPageBuffer);
    } catch (IOException e) {
      logger.error("Error creating Index file reader: ", e);
      throw new RuntimeException(e);
    }
  }

  private void initializeTupleReader(Table table) {
    try {
      String tablePath = DBCatalog.getInstance().getTablePath(table.getName());
      tableChannel = new FileInputStream(tablePath).getChannel();
      tupleReader = new BinaryTupleReader(tableChannel);
    } catch (IOException e) {
      logger.error("Error creating BinaryTupleReader: ", e);
      throw new RuntimeException(e);
    }
  }

  private void positionReaderAtRecord() {
    // get the page id and byte offset of the current record
    RecordId recordId =
        currentleafNode.getEntryAtIndex(currentEntryIndex).getRecordIdAtIndex(currentRecorIndex);
    int pageNumber = recordId.pageNumber();
    int byteOffset = recordId.byteOffset();

    // read the tuple from the table
    try {
      tupleReader.seek(pageNumber, byteOffset);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
    selectedReset.get();
  }

  private Void resetWithClusteredIndex() {
    if (startLeafAddress == -1) {
      return null;
    }
    try {
      currentEntryIndex = startEntryIndex;
      currentRecorIndex = 0;
      positionReaderAtRecord();
      nextTuple = getNextValidTuple();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  private Void resetWithNonClusteredIndex() {
    if (startLeafAddress == -1) {
      return null;
    }
    try {
      currentleafNode = bPlusTree.loadLeafNode(indexChannel, indexPageBuffer, startLeafAddress);
      currentEntryIndex = startEntryIndex;
      currentRecorIndex = 0;
      positionReaderAtRecord();
      nextTuple = getNextValidTuple();
    } catch (IOException e) {
      logger.error("Error resetting index scan: ", e);
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public Tuple getNextTuple() {
    return selectedGetNextTuple.get();
  }

  private Tuple getNextTupleWithClusteredIndex() {

    if (nextTuple == null) {
      return null;
    }

    Tuple toReturn = nextTuple;
    nextTuple = null;

    // read the next tuple
    try {
      nextTuple = getNextValidTuple();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return toReturn;
  }

  private Tuple getNextTupleWithNonClusteredIndex() {

    if (nextTuple == null) {
      return null;
    }

    Tuple toReturn = nextTuple;
    nextTuple = null;

    // read the next tuple
    try {

      // if we have a valid index position
      if (advanceIndexPosition()) {

        // set the reader to the record & get the next tuple
        positionReaderAtRecord();
        nextTuple = getNextValidTuple();

      } else {
        nextTuple = null;
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return toReturn;
  }

  private boolean advanceIndexPosition() throws IOException {
    // increment the record index
    currentRecorIndex++;

    // if we run out of records in the current entry, move to the next entry & reset record index
    RecordEntry currentEntry = currentleafNode.getEntryAtIndex(currentEntryIndex);
    if (currentRecorIndex >= currentEntry.getNumOfRecordIds()) {
      currentEntryIndex++;
      currentRecorIndex = 0;
    }

    // if we run out of entries in the current leaf node, move to the next leaf node & reset entry
    // index
    if (currentEntryIndex >= currentleafNode.getNumOfKeys()) {
      currentleafNode = bPlusTree.getNextLeafNode(indexChannel, indexPageBuffer, currentleafNode);
      currentEntryIndex = 0;
      currentRecorIndex = 0;
    }

    return currentleafNode != null;
  }

  private Tuple getNextValidTuple() throws IOException {
    Tuple candidate = tupleReader.getNextTuple();
    return (candidate != null && isTupleWithinRange(candidate)) ? candidate : null;
  }

  public boolean isTupleWithinRange(Tuple tuple) {
    if (highKey == null) {
      return true;
    }

    int value = tuple.getElementAtIndex(attributeIndex);
    return value <= highKey;
  }

  public void close() {
    try {
      tableChannel.close();
      indexChannel.close();
    } catch (IOException e) {
      logger.error("Error closing table channel: ", e);
    }
  }
}
