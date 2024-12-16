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
  private final int MAX_TUPLE_COUNT_PER_PAGE;

  private final Logger logger = LogManager.getLogger();

  private FileChannel fileChannel = null;
  private final ByteBuffer buffer;
  private int bufferIndex;
  private int tupleCount;

  public BinaryTupleWriter(FileChannel fileChannel, int tupleSize) {
    this.TUPLE_SIZE = tupleSize;
    this.MAX_TUPLE_COUNT_PER_PAGE = (TABLE_PAGE_SIZE - 2 * INT_SIZE) / (tupleSize * INT_SIZE);
    this.fileChannel = fileChannel;
    this.buffer = ByteBuffer.allocate(TABLE_PAGE_SIZE);
    initializeBuffer();
  }

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
    initializeBuffer();
  }

  private void initializeBuffer() {
    // Write initial tuple size and count at the start of the buffer
    buffer.putInt(0, this.TUPLE_SIZE);
    buffer.putInt(INT_SIZE, 0); // Initial tuple count is 0
    this.bufferIndex = 2 * INT_SIZE;
    this.tupleCount = 0;
  }

  @Override
  public void writeTuple(Tuple tuple) {
    // Check if we need to write the current page
    if (tupleCount >= MAX_TUPLE_COUNT_PER_PAGE) {
      writePage();
    }

    // Write the tuple to buffer
    ArrayList<Integer> numbers = tuple.getAllElements();
    if (numbers.size() != TUPLE_SIZE) {
      throw new IllegalArgumentException(
          "Tuple size mismatch. Expected: " + TUPLE_SIZE + ", Got: " + numbers.size());
    }

    for (Integer num : numbers) {
      buffer.putInt(bufferIndex, num);
      bufferIndex += INT_SIZE;
    }

    tupleCount++;
    // Update the tuple count in the buffer header
    buffer.putInt(INT_SIZE, tupleCount);
  }

  private void writePage() {
    try {
      // Fill remaining space with zeros
      while (bufferIndex < TABLE_PAGE_SIZE) {
        buffer.putInt(bufferIndex, 0);
        bufferIndex += INT_SIZE;
      }

      // Write the complete page
      buffer.position(0);
      fileChannel.write(buffer);

      // Reset buffer for next page
      buffer.clear();
      initializeBuffer();

    } catch (IOException e) {
      logger.error("Error writing page: ", e);
    }
  }

  @Override
  public void close() {
    // Write the final page if there's any data
    if (tupleCount > 0) {
      writePage();
    }
  }
}
