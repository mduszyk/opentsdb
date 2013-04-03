package net.opentsdb.core;

import java.util.List;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Comparator;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import static org.hbase.async.Bytes.ByteMap;

import net.opentsdb.stats.Histogram;


public class StorageQueryHbase implements StorageQuery {
    
  private static final Logger LOG = LoggerFactory.getLogger(StorageQueryHbase.class);
  
  /**
   * Keep track of the latency we perceive when doing Scans on HBase.
   * We want buckets up to 16s, with 2 ms interval between each bucket up to
   * 100 ms after we which we switch to exponential buckets.
   */
  static final Histogram scanlatency = new Histogram(16000, (short) 2, 100);
    
  private final TsdbHbase tsdb;
  
  /** ID of the metric being looked up. */
  private byte[] metric;
  private long start_time;
  private long end_time;
  private ArrayList<byte[]> tags;
  private ArrayList<byte[]> group_bys;
  private ByteMap<byte[][]> group_by_values;
  
  public StorageQueryHbase(TsdbHbase tsdb) {
      this.tsdb = tsdb;
  }
  
  public void setMetric(byte[] metric) {
      this.metric = metric;
  }
  
  public void setScanStartTime(long start_time) {
      this.start_time = start_time;
  }
  
  public void setScanEndTime(long end_time) {
      this.end_time = end_time;
  }
  
  public void setTags(ArrayList<byte[]> tags) {
      this.tags = tags;
  }
  
  public void setGroupBys(ArrayList<byte[]> group_bys) {
      this.group_bys = group_bys;
  }
  
  public void setGroupByValues(ByteMap<byte[][]> group_by_values) {
      this.group_by_values = group_by_values;
  }
  
  /**
   * Finds all the {@link Span}s that match this query.
   * This is what actually scans the HBase table and loads the data into
   * {@link Span}s.
   * @return A map from HBase row key to the {@link Span} for that row key.
   * Since a {@link Span} actually contains multiple HBase rows, the row key
   * stored in the map has its timestamp zero'ed out.
   * @throws HBaseException if there was a problem communicating with HBase to
   * perform the search.
   * @throws IllegalArgumentException if bad data was retreived from HBase.
   */
  public TreeMap<byte[], Span> findSpans() throws StorageException {
    final short metric_width = tsdb.metrics.width();
    final TreeMap<byte[], Span> spans =  // The key is a row key from HBase.
      new TreeMap<byte[], Span>(new SpanCmp(metric_width));
    final HashMap<byte[], List<RowSeq>> rows_map = new HashMap<byte[], List<RowSeq>>();
    int nrows = 0;
    int hbase_time = 0;  // milliseconds.
    long starttime = System.nanoTime();
    final Scanner scanner = getScanner();
    try {
      ArrayList<ArrayList<KeyValue>> rows;
      while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
        hbase_time += (System.nanoTime() - starttime) / 1000000;
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] key = row.get(0).key();
          if (Bytes.memcmp(metric, key, 0, metric_width) != 0) {
            throw new IllegalDataException("HBase returned a row that doesn't match"
                + " our scanner (" + scanner + ")! " + row + " does not start"
                + " with " + Arrays.toString(metric));
          }

          List<RowSeq> rowseqs = rows_map.get(key);
          if (rowseqs == null) {
            rowseqs = new ArrayList<RowSeq>();
            rows_map.put(key, rowseqs);
          }
          addRowToSeq(rowseqs, tsdb.compact(row));
          
          Span datapoints = spans.get(key);
          if (datapoints == null) {
            datapoints = new Span();
            datapoints.setSpanViews(rowseqs);
            spans.put(key, datapoints);
          }
          
          nrows++;
          starttime = System.nanoTime();
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Should never be here", e);
    } finally {
      hbase_time += (System.nanoTime() - starttime) / 1000000;
      scanlatency.add(hbase_time);
    }
    LOG.info(this + " matched " + nrows + " rows in " + spans.size() + " spans");
    if (nrows == 0) {
      return null;
    }
    return spans;
  }
  
  /**
   * Adds an HBase row to this span, using a row from a scanner.
   * @param row The compacted HBase row to add to this span.
   * @throws IllegalArgumentException if the argument and this span are for
   * two different time series.
   * @throws IllegalArgumentException if the argument represents a row for
   * data points that are older than those already added to this span.
   */
  private void addRowToSeq(List<RowSeq> rows, final KeyValue row) {
    long last_ts = 0;
    if (rows.size() != 0) {
      // Verify that we have the same metric id and tags.
      final byte[] key = row.key();
      final RowSeq last = rows.get(rows.size() - 1);
      final short metric_width = tsdb.metrics.width();
      final short tags_offset = (short) (metric_width + Const.TIMESTAMP_BYTES);
      final short tags_bytes = (short) (key.length - tags_offset);
      String error = null;
      if (key.length != last.key.length) {
        error = "row key length mismatch";
      } else if (Bytes.memcmp(key, last.key, 0, metric_width) != 0) {
        error = "metric ID mismatch";
      } else if (Bytes.memcmp(key, last.key, tags_offset, tags_bytes) != 0) {
        error = "tags mismatch";
      }
      if (error != null) {
        throw new IllegalArgumentException(error + ". "
            + "This Span's last row key is " + Arrays.toString(last.key)
            + " whereas the row key being added is " + Arrays.toString(key)
            + " and metric_width=" + metric_width);
      }
      last_ts = last.timestamp(last.size() - 1);  // O(1)
      // Optimization: check whether we can put all the data points of `row'
      // into the last RowSeq object we created, instead of making a new
      // RowSeq.  If the time delta between the timestamp encoded in the
      // row key of the last RowSeq we created and the timestamp of the
      // last data point in `row' is small enough, we can merge `row' into
      // the last RowSeq.
      if (RowSeq.canTimeDeltaFit(lastTimestampInRow(metric_width, row)
                                 - last.baseTime())) {
        last.addRow(row);
        return;
      }
    }

    final RowSeq rowseq = new RowSeq(tsdb);
    rowseq.setRow(row);
    if (last_ts >= rowseq.timestamp(0)) {
      LOG.error("New RowSeq added out of order to this Span! Last = " +
                rows.get(rows.size() - 1) + ", new = " + rowseq);
      return;
    }
    rows.add(rowseq);
  }
  
  /**
   * Package private helper to access the last timestamp in an HBase row.
   * @param metric_width The number of bytes on which metric IDs are stored.
   * @param row A compacted HBase row.
   * @return A strictly positive 32-bit timestamp.
   * @throws IllegalArgumentException if {@code row} doesn't contain any cell.
   */
  private static long lastTimestampInRow(final short metric_width,
                                 final KeyValue row) {
    final long base_time = Bytes.getUnsignedInt(row.key(), metric_width);
    final byte[] qual = row.qualifier();
    final short last_delta = (short)
      (Bytes.getUnsignedShort(qual, qual.length - 2) >>> Const.FLAG_BITS);
    return base_time + last_delta;
  }
  
  /**
   * Creates the {@link Scanner} to use for this query.
   */
  private Scanner getScanner() throws HBaseException {
    final short metric_width = tsdb.metrics.width();
    final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
    final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
    // We search at least one row before and one row after the start & end
    // time we've been given as it's quite likely that the exact timestamp
    // we're looking for is in the middle of a row.  Plus, a number of things
    // rely on having a few extra data points before & after the exact start
    // & end dates in order to do proper rate calculation or downsampling near
    // the "edges" of the graph.
    Bytes.setInt(start_row, (int) start_time, metric_width);
    Bytes.setInt(end_row, (end_time == TsdbQuery.UNSET
                           ? -1  // Will scan until the end (0xFFF...).
                           : (int) end_time),
                 metric_width);
    System.arraycopy(metric, 0, start_row, 0, metric_width);
    System.arraycopy(metric, 0, end_row, 0, metric_width);

    final Scanner scanner = tsdb.client.newScanner(tsdb.table);
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    if (tags.size() > 0 || group_bys != null) {
      createAndSetFilter(scanner);
    }
    scanner.setFamily(TsdbHbase.FAMILY);
    return scanner;
  }
  
  /**
   * Sets the server-side regexp filter on the scanner.
   * In order to find the rows with the relevant tags, we use a
   * server-side filter that matches a regular expression on the row key.
   * @param scanner The scanner on which to add the filter.
   */
  void createAndSetFilter(final Scanner scanner) {
    if (group_bys != null) {
      Collections.sort(group_bys, Bytes.MEMCMP);
    }
    final short name_width = tsdb.tag_names.width();
    final short value_width = tsdb.tag_values.width();
    final short tagsize = (short) (name_width + value_width);
    // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
    // and { 4 5 6 9 8 7 }, the regexp will be:
    // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
    final StringBuilder buf = new StringBuilder(
        15  // "^.{N}" + "(?:.{M})*" + "$"
        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
           * (tags.size() + (group_bys == null ? 0 : group_bys.size() * 3))));
    // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

    // Alright, let's build this regexp.  From the beginning...
    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping the metric ID and timestamp.
       .append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
       .append("}");
    final Iterator<byte[]> tags = this.tags.iterator();
    final Iterator<byte[]> group_bys = (this.group_bys == null
                                        ? new ArrayList<byte[]>(0).iterator()
                                        : this.group_bys.iterator());
    byte[] tag = tags.hasNext() ? tags.next() : null;
    byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
    // Tags and group_bys are already sorted.  We need to put them in the
    // regexp in order by ID, which means we just merge two sorted lists.
    do {
      // Skip any number of tags.
      buf.append("(?:.{").append(tagsize).append("})*\\Q");
      if (isTagNext(name_width, tag, group_by)) {
        addId(buf, tag);
        tag = tags.hasNext() ? tags.next() : null;
      } else {  // Add a group_by.
        addId(buf, group_by);
        final byte[][] value_ids = (group_by_values == null
                                    ? null
                                    : group_by_values.get(group_by));
        if (value_ids == null) {  // We don't want any specific ID...
          buf.append(".{").append(value_width).append('}');  // Any value ID.
        } else {  // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
          buf.append("(?:");
          for (final byte[] value_id : value_ids) {
            buf.append("\\Q");
            addId(buf, value_id);
            buf.append('|');
          }
          // Replace the pipe of the last iteration.
          buf.setCharAt(buf.length() - 1, ')');
        }
        group_by = group_bys.hasNext() ? group_bys.next() : null;
      }
    } while (tag != group_by);  // Stop when they both become null.
    // Skip any number of tags before the end.
    buf.append("(?:.{").append(tagsize).append("})*$");
    scanner.setKeyRegexp(buf.toString(), TsdbQuery.CHARSET);
  }
   
  /**
   * Helper comparison function to compare tag name IDs.
   * @param name_width Number of bytes used by a tag name ID.
   * @param tag A tag (array containing a tag name ID and a tag value ID).
   * @param group_by A tag name ID.
   * @return {@code true} number if {@code tag} should be used next (because
   * it contains a smaller ID), {@code false} otherwise.
   */
  private boolean isTagNext(final short name_width,
                            final byte[] tag,
                            final byte[] group_by) {
    if (tag == null) {
      return false;
    } else if (group_by == null) {
      return true;
    }
    final int cmp = Bytes.memcmp(tag, group_by, 0, name_width);
    if (cmp == 0) {
      throw new AssertionError("invariant violation: tag ID "
          + Arrays.toString(group_by) + " is both in 'tags' and"
          + " 'group_bys' in " + this);
    }
    return cmp < 0;
  }
  
  /**
   * Appends the given ID to the given buffer, followed by "\\E".
   */
  private static void addId(final StringBuilder buf, final byte[] id) {
    boolean backslash = false;
    for (final byte b : id) {
      buf.append((char) (b & 0xFF));
      if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
        // So we just terminated the quoted section because we just added \E
        // to `buf'.  So let's put a litteral \E now and start quoting again.
        buf.append("\\\\E\\Q");
      } else {
        backslash = b == '\\';
      }
    }
    buf.append("\\E");
  }
   
  /**
   * Comparator that ignores timestamps in row keys.
   */
  private static final class SpanCmp implements Comparator<byte[]> {

    private final short metric_width;

    public SpanCmp(final short metric_width) {
      this.metric_width = metric_width;
    }

    public int compare(final byte[] a, final byte[] b) {
      final int length = Math.min(a.length, b.length);
      if (a == b) {  // Do this after accessing a.length and b.length
        return 0;    // in order to NPE if either a or b is null.
      }
      int i;
      // First compare the metric ID.
      for (i = 0; i < metric_width; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      // Then skip the timestamp and compare the rest.
      for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
        if (a[i] != b[i]) {
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
        }
      }
      return a.length - b.length;
    }

  }
  
}