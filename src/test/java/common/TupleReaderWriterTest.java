package common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TupleReaderWriterTest {

  @TempDir static Path tempDir;

  static Path tempFile;

  @BeforeAll
  static void SetUp() {
    // Create a temporary directory to store the test files
    tempFile = tempDir.resolve("testInput");
  }

  @Test
  public void testHumanReadableTupleWriterReader() throws IOException {

    int tupleSize = 3;
    Tuple tuple1 = new Tuple("1,2,3");
    Tuple tuple2 = new Tuple("4,5,6");
    Tuple tuple3 = new Tuple("7,8,9");

    // Write the tuples to a file
    HumanReadableTupleWriter writer =
        new HumanReadableTupleWriter(tempFile.toAbsolutePath().toString(), tupleSize);
    writer.writeTuple(tuple1);
    writer.writeTuple(tuple2);
    writer.writeTuple(tuple3);
    writer.close();

    // Read the tuples from the file
    HumanReadableTupleReader reader =
        new HumanReadableTupleReader(tempFile.toAbsolutePath().toString());
    Tuple readTuple1 = reader.getNextTuple();
    Tuple readTuple2 = reader.getNextTuple();
    Tuple readTuple3 = reader.getNextTuple();
    reader.close();

    assertEquals(tuple1, readTuple1);
    assertEquals(tuple2, readTuple2);
    assertEquals(tuple3, readTuple3);
  }

  @Test
  void testBinaryTupleWriterReader() throws IOException {

    int tupleSize = 3;
    Tuple tuple1 = new Tuple("1,2,3");
    Tuple tuple2 = new Tuple("4,5,6");
    Tuple tuple3 = new Tuple("7,8,9");

    // Write the tuples to a file
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    writer.writeTuple(tuple1);
    writer.writeTuple(tuple2);
    writer.writeTuple(tuple3);
    writer.close();

    // Read the tuples from the file
    BinaryTupleReader reader = new BinaryTupleReader(tempFile.toString());
    Tuple readTuple1 = reader.getNextTuple();
    Tuple readTuple2 = reader.getNextTuple();
    Tuple readTuple3 = reader.getNextTuple();
    reader.close();

    assertEquals(tuple1, readTuple1);
    assertEquals(tuple2, readTuple2);
    assertEquals(tuple3, readTuple3);
  }

  @Test
  void testBinaryTupleWriterReaderWithMultiPages() throws IOException {

    int tupleSize = 3;

    // create a list of tuples
    ArrayList<Tuple> tuples = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      tuples.add(new Tuple(i + "," + (i + 1) + "," + (i + 2)));
    }

    // there are 1000 tuples, each tuple has 3 integers, each integer has 4 bytes
    // so the total size of the tuples is 1000 * 3 * 4 = 12000 bytes

    // Write the tuples to a file
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), tupleSize);
    for (Tuple tuple : tuples) {
      writer.writeTuple(tuple);
    }
    writer.close();

    // Read the tuples from the file
    BinaryTupleReader reader = new BinaryTupleReader(tempFile.toString());
    for (Tuple tuple : tuples) {
      Tuple readTuple = reader.getNextTuple();
      assertEquals(tuple, readTuple);
    }
    reader.close();
  }
}
