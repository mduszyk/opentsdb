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
  private int lastDelta;
  
  public DownsampleFilter() {}

  public DownsampleFilter(byte[] interval_buf) {
    this.interval_buf = interval_buf;
    interval = toInt(interval_buf, 0, 4);
    lastDelta = 0;
  }
  
  public void reset() {
    lastDelta = 0;
  }
  
  /**
   * Get int from big endian
   */
  private int toInt(byte[] buf, int i, int j) {
    int val = 0;
    for (int k = i; k < j; k++)
      val = (val << 8) | (buf[k] & 0xFF);
    return val;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    if (v.getQualifierLength() > 0) {
      int offset = v.getQualifierOffset();
      // read big endian qualifier bytes into int variable
      int delta = toInt(v.getBuffer(), offset, offset + 2);
      // get rid of flag bits
      delta = (int) ((delta & 0xFFFFFFFF) >>> FLAG_BITS);
      
      if (lastDelta + interval < delta) {
          lastDelta = delta;
          return ReturnCode.INCLUDE;
      }
    }
    
    return ReturnCode.SKIP;
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
    interval = toInt(interval_buf, 0, 4);
  }

}
