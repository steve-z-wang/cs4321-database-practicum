package index;

import java.io.IOException;
import java.nio.ByteBuffer;

public record RecordId(int pageNumber, int byteOffset) {

  public void serialize(ByteBuffer buffer) throws IOException {
    buffer.putInt(pageNumber);
    buffer.putInt(byteOffset);
  }

  public static RecordId deserialize(ByteBuffer buffer) throws IOException {
    int pageId = buffer.getInt();
    int tupleId = buffer.getInt();
    return new RecordId(pageId, tupleId);
  }
}
