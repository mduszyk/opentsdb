package net.opentsdb.filter;

import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Downsampling filter
 */
public class DownsampleFilter extends FilterBase {
    
  public static final byte[] DOWNSAMPLE_FILTER = {
    'n', 'e', 't', '.', 'o', 'p', 'e', 'n', 't', 's', 'd', 'b', '.', 'f', 'i', 'l', 't', 'e',
    'r', '.', 'D', 'o', 'w', 'n', 's', 'a', 'm', 'p', 'l', 'e', 'F', 'i', 'l', 't', 'e', 'r'};
    
  /** Number of LSBs in time_deltas reserved for flags.  */
  static final short FLAG_BITS = 4;
  
  private byte[] interval_buf;  
  private int interval;
  private int skip;
  private boolean jump;
  
  public DownsampleFilter() {}

  public DownsampleFilter(byte[] interval_buf) {
    this.interval_buf = interval_buf;
    interval = toInt(interval_buf);
    skip = 0;
    jump = false;
  }
  
  public void reset() {
    skip = 0;
    jump = false;
  }
  
  /**
   * Get int from big endian
   */
  private int toInt(byte[] buf) {
    int val = 0;
    val = val | (buf[0] & 0xFF);
    val = (val << 8) | (buf[1] & 0xFF);
    val = (val << 8) | (buf[2] & 0xFF);
    val = (val << 8) | (buf[3] & 0xFF);
    return val;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if (v.getQualifierLength() > 0) {
      // read big endian qualifier bytes into int variable
      int offset = v.getQualifierOffset();
      byte[] buf = v.getBuffer();
      int delta = 0;
      delta = delta | (buf[offset] & 0xFF);
      delta = (delta << 8) | (buf[offset + 1] & 0xFF);
      // get rid of flag bits
      delta = (int) ((delta & 0xFFFFFFFF) >>> FLAG_BITS);
      //System.out.println("delta: " + delta + ", interval: " + interval + ", skip: " + skip);
      if (skip <= delta) {
        skip = delta + interval;
        jump = true;
        return ReturnCode.INCLUDE;
      }
      if (jump) {
        jump = false;
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
    }
    
    return ReturnCode.NEXT_COL;
  }
  
  public KeyValue getNextKeyHint(KeyValue kv) {
    //System.out.println("hint skip: " + skip);
    short qual = (short) ((skip + 1) << FLAG_BITS);
    byte[] qualifier = new byte[2];
    qualifier[0] = (byte) (qual >>> 8);
    qualifier[1] = (byte) (qual & 0xFF);
    return KeyValue.createFirstOnRow(kv.getRow(), kv.getFamily(), qualifier);
  }
  
  public byte[] toByteArray() {
    byte[] array = new byte[42];
    
    final ByteBuffer buf = ByteBuffer.wrap(array);
    buf.put((byte) DOWNSAMPLE_FILTER.length); //1
    buf.put(DOWNSAMPLE_FILTER); //36
    buf.put((byte) interval_buf.length); //1
    buf.put(interval_buf); //4
    
    return array;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    if (filterArguments.size() != 1)
        throw new IllegalArgumentException("Expected 1 argument");
    byte [] interval_buf = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    return new DownsampleFilter(interval_buf);
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.interval_buf);
  }

  public void readFields(DataInput in) throws IOException {
    interval_buf = Bytes.readByteArray(in);
    interval = toInt(interval_buf);
  }

}
