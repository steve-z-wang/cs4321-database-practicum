package common;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * This class contains the logic that read tuples by pages The input files are of binary format The
 * class will utilize java nio to read the files
 */
public class BinaryTupleReader implements TupleReader {
  private final int PAGE_SIZE;
  private final int INT_SIZE = 4;
  private final int TUPLE_SIZE;

  private final FileChannel fileChannel;
  private final ByteBuffer buffer;
  private int remainingTuples;
  private int bufferIndex;

  public BinaryTupleReader(String filePath, int tupleSize) throws IOException {
    this(filePath, tupleSize, 4096);
  }

  public BinaryTupleReader(String filePath, int tupleSize, int pageSize) throws IOException {
    this.PAGE_SIZE = pageSize;
    this.TUPLE_SIZE = tupleSize;

    this.fileChannel = new FileInputStream(filePath).getChannel();
    this.buffer = ByteBuffer.allocate(this.PAGE_SIZE);

    readPage();
  }

  private boolean readPage() throws IOException {
    buffer.clear();
    int bytesRead = this.fileChannel.read(this.buffer);
    if (bytesRead == -1) {
      return false;
    }

    buffer.flip();

    // Guard against file corruption (e.g. not enough bytes for tuple size/remaining count)
    if (buffer.remaining() < 2 * INT_SIZE) {
      throw new IOException("Corrupted page or incomplete file.");
    }

    this.remainingTuples = this.buffer.getInt(INT_SIZE);
    this.bufferIndex = 2 * INT_SIZE;
    return true;
  }

  @Override
  public Tuple getNextTuple() throws IOException {
    if (remainingTuples <= 0) {
      if (!readPage()) {
        return null;
      }
    }

    ArrayList<Integer> tupleData = new ArrayList<>();
    for (int i = 0; i < this.TUPLE_SIZE; i++) {
      tupleData.add(buffer.getInt(this.bufferIndex));
      bufferIndex += INT_SIZE;
    }

    remainingTuples--;
    return new Tuple(tupleData);
  }

  @Override
  public void reset() throws IOException {
    fileChannel.position(0);
    readPage();
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
  }

  @Override
  public int getTupleSize() {
    return this.TUPLE_SIZE;
  }
}
