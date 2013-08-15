/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hadoop.mapred;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CompressedFileSplit extends FileSplit {
  private Path file;
  private long start;
  private long length;
  private String[] hosts;
  private long uncStart;
  private long uncLength;
  private boolean ignoreFirstLine;

  public CompressedFileSplit() {}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public CompressedFileSplit(Path file, long start, long length, String[] hosts, long uncStart, long uncLength, boolean ignoreFirstLine) {
    super(file, start, length, hosts);

    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.uncStart = uncStart;
    this.uncLength = uncLength;
    this.ignoreFirstLine = ignoreFirstLine;
  }

  /** The first byte's position in the uncompressed stream */
  public long getUncStart() { return uncStart; }

  /** The number of uncompressed bytes to process from the stream */
  public long getUncLength() { return uncLength; }

  /** Whether to ignore the first line of the file */
  public boolean getIgnoreFirstLine() { return ignoreFirstLine; }

  @Override
  public String toString() { return file + ":" + start + "+" + length + " (" + uncStart + "+" + uncLength + ") ignore first line " + ignoreFirstLine; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  //@Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
    out.writeLong(uncStart);
    out.writeLong(uncLength);
    out.writeLong(ignoreFirstLine ? 1 : 0);
  }

  //@Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    uncStart = in.readLong();
    uncLength = in.readLong();
    ignoreFirstLine = in.readLong() == 1;
    hosts = null;
  }
}

