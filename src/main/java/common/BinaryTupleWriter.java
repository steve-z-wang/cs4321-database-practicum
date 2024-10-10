package common;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BinaryTupleWriter implements TupleWriter {
  private final int PAGE_SIZE;
  private final int INT_SIZE = 4;
  private final int TUPLE_SIZE;
  private final int MAX_TUPLE_COUNT_PER_PAGE; // the maximum of tuple that can be hold on a page

  private final Logger logger = LogManager.getLogger();

  private final FileChannel fileChannel;
  private final ByteBuffer buffer;
  private int bufferIndex;
  private int tupleCount;

  public BinaryTupleWriter(String filePath, int tupleSize) throws IOException {
    this(filePath, tupleSize, 4096);
  }

  public BinaryTupleWriter(String filePath, int tupleSize, int pageSize) throws IOException {
    this.TUPLE_SIZE = tupleSize;
    this.PAGE_SIZE = pageSize;
    this.MAX_TUPLE_COUNT_PER_PAGE = (PAGE_SIZE - 2 * INT_SIZE) / (tupleSize * INT_SIZE);

    this.fileChannel = new FileOutputStream(filePath).getChannel();
    this.buffer = ByteBuffer.allocate(this.PAGE_SIZE);

    // start with an index
    this.bufferIndex = 2 * INT_SIZE;
    this.tupleCount = 0;
  }

  @Override
  public void writeTuple(Tuple tuple) {

    // check for remaining space on page
    if (tupleCount >= MAX_TUPLE_COUNT_PER_PAGE) {
      writePage();
    }

    // write the tuple on buffer
    ArrayList<Integer> numbers = tuple.getAllElements();
    int newBufferIndex = bufferIndex;
    for (Integer num : numbers) {
      buffer.putInt(newBufferIndex, num);
      newBufferIndex += INT_SIZE;
    }
    assert newBufferIndex == bufferIndex + TUPLE_SIZE * INT_SIZE;

    // update index & count
    bufferIndex = newBufferIndex;
    tupleCount++;
  }

  private void writePage() {
    for (; bufferIndex < PAGE_SIZE; bufferIndex += INT_SIZE) {
      buffer.putInt(bufferIndex, 0);
    }

    buffer.putInt(0, this.TUPLE_SIZE);
    buffer.putInt(INT_SIZE, this.tupleCount);

    try {
      fileChannel.write(buffer);
    } catch (IOException e) {
      logger.error("Error writing page: ", e);
    }

    buffer.clear();
    bufferIndex = 2 * INT_SIZE;
    tupleCount = 0;
  }

  @Override
  public void close() {

    // if we start writing a new page
    if (tupleCount > 0) {
      writePage();
    }

    try {
      fileChannel.close();
    } catch (IOException e) {
      logger.error("Error closing BinaryTupleWriter: ", e);
    }
  }

  @Override
  public int getTupleSize() {
    return this.TUPLE_SIZE;
  }
}
