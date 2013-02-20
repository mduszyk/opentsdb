package net.opentsdb.filter;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Downsampling filter
 */
public class DownsampleFilter extends FilterBase {
    
  public static final byte[] DOWNSAMPLE_FILTER = "net.opentsdb.filter.DownsampleFilter"
    .getBytes("ISO-8859-1");
    
  /** Number of LSBs in time_deltas reserved for flags.  */
  static final short FLAG_BITS = 4;
  
  private byte[] interval_buf;  
  private int interval;
  private int lastDelta;

  public DownsampleFilter(byte[] interval_buf) {
    this.interval_buf = interval_buf;
    interval = toInt(interval_buf);
    lastDelta = 0;
  }
  
  /**
   * Get int from big endian
   */
  private int toInt(byte[] buf) {
    int val = 0;
    for (int i = 0; i < buf.length; i++)
      val = (val << 8) | (buf[i] & 0xFF);
    return val;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    int length = v.getQualifierLength();
    if (length > 0) {
      int offset = v.getQualifierOffset();
      byte[] buf = v.getBuffer();
      
      // read big endian qualifier bytes into int variable
      int delta = toInt(buf);
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
    byte[] array = new byte[(1 + 36 + 1 + 4)];  
    
    final ByteBuffer buf = ByteBuffer.wrap(array);
    buf.put((byte) DOWNSAMPLE_FILTER.length); //1
    buf.put(DOWNSAMPLE_FILTER); //36
    buf.writeByte((byte) interval_buf.length); //1
    buf.writeBytes(interval_buf); //4
    
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
