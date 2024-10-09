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
public class TupleReader {
  private final int INT_SIZE = 4;

  private FileChannel fileChannel;
  private ByteBuffer buffer;
  private int pageSize;
  private int tupleSize;
  private int bufferIndex;
  private int lastBufferIndex;

  public TupleReader(String fileName, int pageSize) throws IOException {
    this.fileChannel = new FileInputStream(fileName).getChannel();
    this.buffer = ByteBuffer.allocate(pageSize);
    this.pageSize = pageSize;

    // Read the first page to the buffer
    readPage();
  }

  /**
   * read the page, return false if at the end of file
   *
   * @return
   * @throws IOException
   */
  private boolean readPage() throws IOException {
    buffer.clear();
    if (this.fileChannel.read(this.buffer) == -1) {
      return false;
    }

    buffer.flip();
    this.tupleSize = this.buffer.getInt(0);
    int numOfTuples = this.buffer.getInt(INT_SIZE);

    this.bufferIndex = 2 * INT_SIZE;
    this.lastBufferIndex = 2 * INT_SIZE + (numOfTuples - 1) * tupleSize * INT_SIZE;

    return true;
  }

  /**
   * Read and return the next tuple from the buffer
   *
   * @return the next tuple
   */
  public Tuple getNextTuple() throws IOException {

    // get next index
    if (bufferIndex > lastBufferIndex) {
      if (!readPage()) {
        return null;
      }
    }

    // fill the next tuple
    ArrayList<Integer> tupleData = new ArrayList<>();
    for (int i = 0; i < this.tupleSize; i++) {
      tupleData.add(buffer.getInt(this.bufferIndex));
      bufferIndex += INT_SIZE;
    }

    return new Tuple(tupleData);
  }

  /** Start from the beginning of the file */
  public void reset() throws IOException {
    fileChannel.position(0);
    readPage();
  }

  public void close() throws IOException {
    if (fileChannel != null) {
      fileChannel.close();
    }
  }
}
