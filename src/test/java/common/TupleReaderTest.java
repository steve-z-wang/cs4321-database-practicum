package common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TupleReaderTest {

  private static final String TEST_FILE = "testFile.bin";
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;

  @BeforeEach
  void setUp() throws IOException {
    // Create a test file with some binary data representing tuples
    File file = new File(TEST_FILE);
    fileOutputStream = new FileOutputStream(file);
    fileChannel = fileOutputStream.getChannel();

    // Simulate a page with tuple size and tuple data
    ByteBuffer buffer = ByteBuffer.allocate(32); // Example page size
    buffer.putInt(3); // Tuple size (3 integers per tuple)
    buffer.putInt(2); // Number of tuples in the page

    // Tuple 1: [1, 2, 3]
    buffer.putInt(1);
    buffer.putInt(2);
    buffer.putInt(3);

    // Tuple 2: [4, 5, 6]
    buffer.putInt(4);
    buffer.putInt(5);
    buffer.putInt(6);

    buffer.flip();
    fileChannel.write(buffer);
    fileChannel.close();
  }

  @AfterEach
  void tearDown() throws IOException {
    // Clean up the test file
    File file = new File(TEST_FILE);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  void testGetNextTuple() throws IOException {
    // Create TupleReader with test file
    TupleReader tupleReader = new TupleReader(TEST_FILE, 32);

    // Get first tuple and verify data
    Tuple tuple1 = tupleReader.getNextTuple();
    assertNotNull(tuple1);
    assertEquals(Arrays.asList(1, 2, 3), tuple1.getAllElements());

    // Get second tuple and verify data
    Tuple tuple2 = tupleReader.getNextTuple();
    assertNotNull(tuple2);
    assertEquals(Arrays.asList(4, 5, 6), tuple2.getAllElements());

    // There should be no more tuples, so the next call should return null
    Tuple tuple3 = tupleReader.getNextTuple();
    assertNull(tuple3);

    // Close the reader
    tupleReader.close();
  }

  @Test
  void testReset() throws IOException {
    TupleReader tupleReader = new TupleReader(TEST_FILE, 32);

    // Read the first tuple
    Tuple tuple1 = tupleReader.getNextTuple();
    assertNotNull(tuple1);
    assertEquals(Arrays.asList(1, 2, 3), tuple1.getAllElements());

    // Reset and read again from the beginning
    tupleReader.reset();
    Tuple resetTuple1 = tupleReader.getNextTuple();
    assertNotNull(resetTuple1);
    assertEquals(Arrays.asList(1, 2, 3), resetTuple1.getAllElements());

    // Close the reader
    tupleReader.close();
  }

  @Test
  void testFileNotFound() {
    assertThrows(IOException.class, () -> new TupleReader("nonexistent_file.bin", 32));
  }
}
