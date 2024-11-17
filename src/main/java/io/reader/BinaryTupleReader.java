package io.reader;

import static config.PhysicalPlanConfig.INT_SIZE;

import config.PhysicalPlanConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class contains the logic that read tuples by pages The input files are of binary format The
 * class will utilize java nio to read the files
 */
public class BinaryTupleReader extends TupleReader {

  private static final Logger logger = LogManager.getLogger(BinaryTupleReader.class);

  private static final int DEFAULT_PAGE_SIZE = PhysicalPlanConfig.PAGE_SIZE;
  private int TUPLE_SIZE;

  private FileChannel fileChannel = null;
  private final ByteBuffer buffer;
  private int remainingTuples;
  private int bufferIndex;

  public BinaryTupleReader(FileChannel fileChannel) {
    this(fileChannel, DEFAULT_PAGE_SIZE);
  }

  public BinaryTupleReader(FileChannel fileChannel, int BytesPerPage) {
    this.fileChannel = fileChannel;
    this.buffer = ByteBuffer.allocate(BytesPerPage);
    readPage();
  }

  // for backward compatibility
  public BinaryTupleReader(String filePath) {
    this(filePath, DEFAULT_PAGE_SIZE);
  }

  // for backward compatibility
  public BinaryTupleReader(String filePath, int pageSize) {
    try {
      this.fileChannel = new FileInputStream(filePath).getChannel();
    } catch (IOException e) {
      logger.error("Error creating BinaryTupleReader: ", e);
    }

    this.buffer = ByteBuffer.allocate(pageSize);
    readPage();
  }

  /**
   * Read a page from the file
   * @return true if there is a page to read, false otherwise
   */
  private boolean readPage() {
    buffer.clear();

    try {
      int bytesRead = this.fileChannel.read(this.buffer);
      if (bytesRead == -1) {
        return false;
      }
    } catch (IOException e) {
      logger.error("Error reading page: ", e);
      return false;
    }

    buffer.flip();

    this.TUPLE_SIZE = this.buffer.getInt(0);
    this.remainingTuples = this.buffer.getInt(INT_SIZE);
    this.bufferIndex = 2 * INT_SIZE;
    return true;
  }

  @Override
  public Tuple getNextTuple() {
    if (remainingTuples <= 0) {
      if (!readPage()) {
        return null;
      }
    }

    // read the tuple data
    ArrayList<Integer> tupleData = new ArrayList<>();
    for (int i = 0; i < this.TUPLE_SIZE; i++) {
      tupleData.add(buffer.getInt(this.bufferIndex));
      bufferIndex += INT_SIZE;
    }

    remainingTuples--;
    return new Tuple(tupleData);
  }

  @Override
  public void reset() {
    try {
      this.fileChannel.position(0);
    } catch (IOException e) {
      logger.error("Error resetting BinaryTupleReader: ", e);
    }
    readPage();
  }

  @Override
  public void reset(int index) {

    // calculate the page number and the offset within the page
    int pageSize = this.buffer.capacity();
    int tuplesPerPage = (pageSize - 2 * INT_SIZE) / (INT_SIZE * this.TUPLE_SIZE);
    int pageNumber = index / tuplesPerPage;
    int offset = index % tuplesPerPage;

    // suppose index is 4 and tuplesPerPage is 2
    // pageNumber = 4 / 2 = 2,
    // offset = 4 % 2 = 0

    try {
      this.fileChannel.position((long) pageNumber * pageSize);
    } catch (IOException e) {
      logger.error("Error resetting BinaryTupleReader: ", e);
    }

    readPage();

    // skip the tuples before the desired tuple
    this.remainingTuples -= offset;
    this.bufferIndex += offset * INT_SIZE * this.TUPLE_SIZE;
  }

  @Override
  public void close() {
    try {
      this.fileChannel.close();
    } catch (IOException e) {
      logger.error("Error closing BinaryTupleReader: ", e);
    }
  }
}
