package index;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class BPlusTreeNode {
  protected int address; // address of this node in the index file
  protected boolean isLeaf;

  public void setAddress(int address) {
    this.address = address;
  }

  public int getAddress() {
    return address;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public abstract int getKeyAtIndex(int index);

  public abstract int getNumOfKeys();

  public abstract void serialize(ByteBuffer buffer) throws IOException;
}
