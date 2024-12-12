package index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RecordEntry {
  private int key;
  private List<RecordId> recordIds;

  public RecordEntry(int key) {
    this.key = key;
    this.recordIds = new ArrayList<>();
  }

  public void addRecordId(RecordId recordId) {
    recordIds.add(recordId);
  }

  public RecordId getRecordIdAtIndex(int index) {
    return recordIds.get(index);
  }

  public int getNumOfRecordIds() {
    return recordIds.size();
  }

  public int getKey() {
    return key;
  }

  public void serialize(ByteBuffer buffer) throws IOException {

    // write key
    buffer.putInt(key);

    // write number of record ids
    buffer.putInt(recordIds.size());

    for (RecordId recordId : recordIds) {
      recordId.serialize(buffer);
    }
  }

  public static RecordEntry deserialize(ByteBuffer buffer) throws IOException {

    // get key
    int key = buffer.getInt();

    RecordEntry entry = new RecordEntry(key);

    // get number of record ids
    int numRecordIds = buffer.getInt();

    for (int i = 0; i < numRecordIds; i++) {
      RecordId recordId = RecordId.deserialize(buffer);
      entry.addRecordId(recordId);
    }

    return entry;
  }
}
