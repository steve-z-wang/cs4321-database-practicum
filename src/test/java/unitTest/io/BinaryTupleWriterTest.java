package unitTest.io;

import static org.junit.jupiter.api.Assertions.*;

import config.PhysicalPlanConfig;
import io.writer.BinaryTupleWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import model.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BinaryTupleWriterTest {

  @AfterEach
  void resetPageSize() {
    PhysicalPlanConfig.getInstance().setPageSize(4096); // Reset to default
  }

  @Test
  void writeSingleTupleFromArrayList() throws IOException {
    Path tempFile = Files.createTempFile("test", ".bin");
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    Tuple tuple = new Tuple(new ArrayList<>(Arrays.asList(1, 2, 3)));
    writer.writeTuple(tuple);
    writer.close();

    byte[] data = Files.readAllBytes(tempFile);

    // Check the file size is 4096 bytes (one full page)
    assertEquals(4096, data.length);

    // Verify that the first few bytes are the correct data
    ByteBuffer buffer = ByteBuffer.wrap(data);

    // Tuple size
    assertEquals(3, buffer.getInt(0)); // First 4 bytes for tuple size

    // Tuple count
    assertEquals(1, buffer.getInt(4)); // Next 4 bytes for tuple count

    // Tuple data (1, 2, 3)
    assertEquals(1, buffer.getInt(8)); // Next 4 bytes for the first element
    assertEquals(2, buffer.getInt(12)); // Next 4 bytes for the second element
    assertEquals(3, buffer.getInt(16)); // Next 4 bytes for the third element

    // Verify the rest of the file is padded with zeros
    for (int i = 20; i < data.length; i++) {
      assertEquals(0, data[i]);
    }
  }

  @Test
  void writeSingleTupleFromString() throws IOException {
    Path tempFile = Files.createTempFile("test", ".bin");
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    Tuple tuple = new Tuple("1,2,3");
    writer.writeTuple(tuple);
    writer.close();

    byte[] data = Files.readAllBytes(tempFile);
    assertEquals(4096, data.length);
    assertEquals(3, ByteBuffer.wrap(data).getInt(0));
    assertEquals(1, ByteBuffer.wrap(data).getInt(4));
    assertEquals(1, ByteBuffer.wrap(data).getInt(8));
    assertEquals(2, ByteBuffer.wrap(data).getInt(12));
    assertEquals(3, ByteBuffer.wrap(data).getInt(16));
  }

  @Test
  void writeMultipleTuplesFromArrayList() throws IOException {
    Path tempFile = Files.createTempFile("test", ".bin");
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    Tuple tuple1 = new Tuple(new ArrayList<>(Arrays.asList(1, 2, 3)));
    Tuple tuple2 = new Tuple(new ArrayList<>(Arrays.asList(4, 5, 6)));
    writer.writeTuple(tuple1);
    writer.writeTuple(tuple2);
    writer.close();

    byte[] data = Files.readAllBytes(tempFile);
    assertEquals(4096, data.length);
    assertEquals(3, ByteBuffer.wrap(data).getInt(0));
    assertEquals(2, ByteBuffer.wrap(data).getInt(4));
    assertEquals(1, ByteBuffer.wrap(data).getInt(8));
    assertEquals(2, ByteBuffer.wrap(data).getInt(12));
    assertEquals(3, ByteBuffer.wrap(data).getInt(16));
    assertEquals(4, ByteBuffer.wrap(data).getInt(20));
    assertEquals(5, ByteBuffer.wrap(data).getInt(24));
    assertEquals(6, ByteBuffer.wrap(data).getInt(28));
  }

  @Test
  void writeMultipleTuplesFromString() throws IOException {
    Path tempFile = Files.createTempFile("test", ".bin");
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    Tuple tuple1 = new Tuple("1,2,3");
    Tuple tuple2 = new Tuple("4,5,6");
    writer.writeTuple(tuple1);
    writer.writeTuple(tuple2);
    writer.close();

    byte[] data = Files.readAllBytes(tempFile);
    assertEquals(4096, data.length);
    assertEquals(3, ByteBuffer.wrap(data).getInt(0));
    assertEquals(2, ByteBuffer.wrap(data).getInt(4));
    assertEquals(1, ByteBuffer.wrap(data).getInt(8));
    assertEquals(2, ByteBuffer.wrap(data).getInt(12));
    assertEquals(3, ByteBuffer.wrap(data).getInt(16));
    assertEquals(4, ByteBuffer.wrap(data).getInt(20));
    assertEquals(5, ByteBuffer.wrap(data).getInt(24));
    assertEquals(6, ByteBuffer.wrap(data).getInt(28));
  }

  @Test
  void writeTuplesExceedingPageSize() throws IOException {
    Path tempFile = Files.createTempFile("test", ".bin");

    PhysicalPlanConfig physicalPlanConfig = PhysicalPlanConfig.getInstance();
    physicalPlanConfig.setPageSize(16);

    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 1);
    for (int i = 0; i < 10; i++) {
      writer.writeTuple(new Tuple(new ArrayList<>(Arrays.asList(i))));
    }
    writer.close();

    byte[] data = Files.readAllBytes(tempFile);

    assertEquals(10 / (16 / 4 - 2) * 16, data.length);
    assertEquals(1, ByteBuffer.wrap(data).getInt(0));
    assertEquals(2, ByteBuffer.wrap(data).getInt(4));
    assertEquals(0, ByteBuffer.wrap(data).getInt(8));
    assertEquals(1, ByteBuffer.wrap(data).getInt(12));
    assertEquals(1, ByteBuffer.wrap(data).getInt(16));
    assertEquals(2, ByteBuffer.wrap(data).getInt(20));
    assertEquals(2, ByteBuffer.wrap(data).getInt(24));
    assertEquals(3, ByteBuffer.wrap(data).getInt(28));
  }

  @Test
  void closeWithoutWriting() throws IOException {
    Path tempFile = Files.createTempFile("test", ".bin");
    BinaryTupleWriter writer = new BinaryTupleWriter(tempFile.toString(), 3);
    writer.close();

    byte[] data = Files.readAllBytes(tempFile);
    assertEquals(0, data.length);
  }
}
