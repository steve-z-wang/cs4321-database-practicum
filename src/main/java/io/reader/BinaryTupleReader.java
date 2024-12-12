package io.reader;

import static utils.DBConstants.INT_SIZE;
import static utils.DBConstants.TABLE_PAGE_SIZE;

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

  private int tupleSize;
  private int tuplesPerPage;
  private int tuplesInCurrentPage; // number of tuples in the current page
  private boolean isEmptyPage;
  private boolean isLastPage;

  private FileChannel fileChannel = null;
  private final ByteBuffer buffer;
  private int currentPageNumber;
  private int currentTupleNumber;

  private Tuple nextTuple;

  public BinaryTupleReader(FileChannel fileChannel) throws IOException {
    logger.info("Creating BinaryTupleReader");

    this.fileChannel = fileChannel;
    this.buffer = ByteBuffer.allocate(TABLE_PAGE_SIZE);
    seek(0, 0);
  }

  private void loadPage(int pageNumber) throws IOException {
    this.fileChannel.position((long) pageNumber * TABLE_PAGE_SIZE);

    buffer.clear();
    int bytesRead = this.fileChannel.read(this.buffer);

    // if the buffer is empty, then we have reached the end of the file
    this.isEmptyPage = false;
    if (bytesRead == -1) {
      this.isEmptyPage = true;
      return;
    }

    buffer.flip();

    this.tupleSize = this.buffer.getInt(0);
    this.tuplesInCurrentPage = this.buffer.getInt(INT_SIZE);
    this.tuplesPerPage = (TABLE_PAGE_SIZE - 2 * INT_SIZE) / (INT_SIZE * this.tupleSize);

    this.isLastPage = false;
    if (this.tuplesInCurrentPage < this.tuplesPerPage) {
      this.isLastPage = true;
    }
  }

  private Tuple readTuple(int tupleNumber) {

    // read the tuple data
    ArrayList<Integer> integers = new ArrayList<>();

    int headerSize = 2 * INT_SIZE;
    int bufferIndex = headerSize + tupleNumber * this.tupleSize * INT_SIZE;

    // Add bounds checking
    if (bufferIndex + (this.tupleSize * INT_SIZE) > buffer.capacity()) {
      throw new IllegalStateException("Attempting to read beyond buffer capacity");
    }

    for (int i = 0; i < this.tupleSize; i++) {
      integers.add(buffer.getInt(bufferIndex));
      bufferIndex += INT_SIZE;
    }
    return new Tuple(integers);
  }

  @Override
  public Tuple getNextTuple() throws IOException {
    if (this.nextTuple == null) {
      return null;
    }

    Tuple toReturn = this.nextTuple;

    // if we have already read all the tuples in the current page, then load the next page
    int nextTupleNumber = this.currentTupleNumber + 1;
    if (nextTupleNumber >= this.tuplesInCurrentPage) {

      // if we have already reach the last page, then there are no more tuples
      if (isLastPage) {
        this.nextTuple = null;
        return toReturn;
      }

      loadPage(++this.currentPageNumber);

      this.currentTupleNumber = 0;
      this.nextTuple = readTuple(this.currentTupleNumber);
    } else {

      this.nextTuple = readTuple(++currentTupleNumber);
    }

    return toReturn;
  }

  /**
   * Seek to a specific tuple in the file
   *
   * @param pageNumber the page number
   * @param tupleNumber the tuple number
   */
  public void seek(int pageNumber, int tupleNumber) throws IOException {
    loadPage(pageNumber);
    this.currentPageNumber = pageNumber;
    this.currentTupleNumber = tupleNumber;

    if (isEmptyPage) {
      this.nextTuple = null;
      return;
    }

    this.nextTuple = readTuple(tupleNumber);
  }

  @Override
  public void reset() throws IOException {
    seek(0, 0);
  }

  // @Override
  public void reset(int index) throws IOException {
    currentPageNumber = index / tuplesPerPage;
    currentTupleNumber = index % tuplesPerPage;

    seek(currentPageNumber, currentTupleNumber);
  }

  public int getCurrentPageNumber() {
    return currentPageNumber;
  }

  public int getCurrentTupleNumber() {
    return currentTupleNumber;
  }

  public FileChannel getFileChannel() {
    return fileChannel;
  }
}
