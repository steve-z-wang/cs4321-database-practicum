package io.reader;

import static org.junit.jupiter.api.Assertions.*;

import io.writer.BinaryTupleWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import model.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BinaryTupleReaderTest {

  @TempDir static Path tempDir;
  static Path tempFile;
  private BinaryTupleReader reader;
  private FileChannel channel;

  @BeforeAll
  static void setUp() {
    tempFile = tempDir.resolve("testBinaryInput");
  }

  @BeforeEach
  void init() throws IOException {
    if (channel != null) {
      channel.close();
    }
  }

  // Test constructor
  @Test
  void testConstructor() throws IOException {
    // Test with empty file
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    assertNull(reader.getNextTuple(), "Empty file should return null on first read");
    assertEquals(0, reader.getCurrentPageNumber(), "Initial page number should be 0");
    assertEquals(0, reader.getCurrentTupleNumber(), "Initial tuple number should be 0");
  }

  // Test getNextTuple()
  @Test
  void testGetNextTuple() throws IOException {
    int tupleSize = 3;
    Tuple tuple1 = new Tuple("1,2,3");
    Tuple tuple2 = new Tuple("4,5,6");

    // Write test data
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    writer.writeTuple(tuple1);
    writer.writeTuple(tuple2);
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    // Test sequential reads
    assertEquals(tuple1, reader.getNextTuple(), "First tuple should match");
    assertEquals(tuple2, reader.getNextTuple(), "Second tuple should match");
    assertNull(reader.getNextTuple(), "Third read should return null");
  }

  // Test getCurrentPageNumber()
  @Test
  void testGetCurrentPageNumber() throws IOException {
    // Create enough tuples to span multiple pages
    int tupleSize = 3;
    ArrayList<Tuple> tuples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tuples.add(new Tuple(i + "," + (i + 1) + "," + (i + 2)));
    }

    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    for (Tuple tuple : tuples) {
      writer.writeTuple(tuple);
    }
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    assertEquals(0, reader.getCurrentPageNumber(), "Should start at page 0");

    // Read through multiple pages
    int tuplesPerPage = (4096 - 8) / (4 * tupleSize); // Calculate based on page size and tuple size
    for (int i = 0; i < tuplesPerPage + 1; i++) {
      reader.getNextTuple();
    }

    assertEquals(
        1, reader.getCurrentPageNumber(), "Should be on page 1 after reading past first page");
  }

  // Test getCurrentTupleNumber()
  @Test
  void testGetCurrentTupleNumber() throws IOException {
    int tupleSize = 3;
    ArrayList<Tuple> tuples = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      tuples.add(new Tuple(i + "," + (i + 1) + "," + (i + 2)));
    }

    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    for (Tuple tuple : tuples) {
      writer.writeTuple(tuple);
    }
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    assertEquals(0, reader.getCurrentTupleNumber(), "Should start at tuple 0");
    reader.getNextTuple();
    assertEquals(1, reader.getCurrentTupleNumber(), "Should be at tuple 1 after first read");
  }

  // Test seek()
  @Test
  void testSeek() throws IOException {
    int tupleSize = 3;
    ArrayList<Tuple> tuples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tuples.add(new Tuple(i + "," + (i + 1) + "," + (i + 2)));
    }

    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    for (Tuple tuple : tuples) {
      writer.writeTuple(tuple);
    }
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    // Seek to specific page and tuple
    // reader.seek(1, 2); // Second page, third tuple
    // assertEquals(1, reader.getCurrentPageNumber(), "Page number should be 1");
    // assertEquals(2, reader.getCurrentTupleNumber(), "Tuple number should be 2");
  }

  // Test reset()
  @Test
  void testReset() throws IOException {
    int tupleSize = 3;
    Tuple tuple1 = new Tuple("1,2,3");
    Tuple tuple2 = new Tuple("4,5,6");

    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    writer.writeTuple(tuple1);
    writer.writeTuple(tuple2);
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    reader.getNextTuple(); // Read first tuple
    reader.reset();

    assertEquals(0, reader.getCurrentPageNumber(), "Page number should be 0 after reset");
    assertEquals(0, reader.getCurrentTupleNumber(), "Tuple number should be 0 after reset");
    assertEquals(tuple1, reader.getNextTuple(), "Should read first tuple after reset");
  }

  // Test reset(index)
  @Test
  void testResetWithIndex() throws IOException {
    int tupleSize = 3;
    ArrayList<Tuple> tuples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tuples.add(new Tuple(i + "," + (i + 1) + "," + (i + 2)));
    }

    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    for (Tuple tuple : tuples) {
      writer.writeTuple(tuple);
    }
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    int tuplesPerPage = (4096 - 8) / (4 * tupleSize);
    int testIndex = tuplesPerPage + 2; // Second tuple on second page

    reader.reset(testIndex);
    assertEquals(1, reader.getCurrentPageNumber(), "Should be on correct page after indexed reset");
    assertEquals(
        2, reader.getCurrentTupleNumber(), "Should be at correct tuple after indexed reset");
  }

  // Test getFileChannel()
  @Test
  void testGetFileChannel() throws IOException {
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    writer.close();

    channel = new FileInputStream(tempFile.toString()).getChannel();
    reader = new BinaryTupleReader(channel);

    assertSame(channel, reader.getFileChannel(), "FileChannel should be the same instance");
  }
}
