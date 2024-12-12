package io.writer;

import static utils.DBConstants.INT_SIZE;
import static utils.DBConstants.TABLE_PAGE_SIZE;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BinaryTupleWriter extends TupleWriter {
  private final int TUPLE_SIZE;
  private final int MAX_TUPLE_COUNT_PER_PAGE; // the maximum of tuple that can be hold on a page

  private final Logger logger = LogManager.getLogger();

  private FileChannel fileChannel = null;
  private final ByteBuffer buffer;
  private int bufferIndex;
  private int tupleCount;

  // Primary constructor that handles all initialization
  public BinaryTupleWriter(FileChannel fileChannel, int tupleSize) {
    this.TUPLE_SIZE = tupleSize;
    this.MAX_TUPLE_COUNT_PER_PAGE = (TABLE_PAGE_SIZE - 2 * INT_SIZE) / (tupleSize * INT_SIZE);
    this.fileChannel = fileChannel;
    this.buffer = ByteBuffer.allocate(TABLE_PAGE_SIZE);
    this.bufferIndex = 2 * INT_SIZE;
    this.tupleCount = 0;
  }

  // For backward compatibility
  // TODO: migrage all to use the fileChannel object creator
  public BinaryTupleWriter(String filePath, int tupleSize) {
    this.TUPLE_SIZE = tupleSize;
    this.MAX_TUPLE_COUNT_PER_PAGE = (TABLE_PAGE_SIZE - 2 * INT_SIZE) / (tupleSize * INT_SIZE);

    try {
      Path path = Paths.get(filePath);
      Files.createDirectories(path.getParent());
      this.fileChannel = new FileOutputStream(filePath).getChannel();
    } catch (IOException e) {
      logger.error("Error creating BinaryTupleWriter: ", e);
    }

    this.buffer = ByteBuffer.allocate(TABLE_PAGE_SIZE);

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
    for (; bufferIndex < TABLE_PAGE_SIZE; bufferIndex += INT_SIZE) {
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
}
