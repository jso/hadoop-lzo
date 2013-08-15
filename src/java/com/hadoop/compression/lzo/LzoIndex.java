/*
 * This file is part of Hadoop-Gpl-Compression.
 *
 * Hadoop-Gpl-Compression is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Hadoop-Gpl-Compression is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hadoop-Gpl-Compression.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.hadoop.compression.lzo;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Represents the lzo index.
 */
public class LzoIndex {
  protected static final Log LOG = LogFactory.getLog(LzoIndex.class);

  public static final String LZO_INDEX_SUFFIX = ".index";
  public static final String LZO_TMP_INDEX_SUFFIX = ".index.tmp";
  public static final long NOT_FOUND = -1;

  private long[] blockPositions_;
  private long[] streamOffsets_;
  private long[] uncBlockLengths_;

  /**
   * Create an empty index, typically indicating no index file exists.
   */
  public LzoIndex() { }

  /**
   * Create an index specifying the number of LZO blocks in the file.
   * @param blocks The number of blocks in the LZO file the index is representing.
   */
  public LzoIndex(int blocks) {
    blockPositions_ = new long[blocks];
    streamOffsets_ = new long[blocks];
    uncBlockLengths_ = new long[blocks];
  }

  /**
   * Set the position for the block.
   *
   * @param blockNumber Block to set pos for.
   * @param pos Position.
   */
  public void set(int blockNumber, long pos, long offset, long len) {
    blockPositions_[blockNumber] = pos;
    streamOffsets_[blockNumber] = offset;
    uncBlockLengths_[blockNumber] = len;
  }

  public void logstate() {
    for(int i = 0; i < getNumberOfBlocks(); i++) {
      LOG.info("block " + i + ": file pos " + getPosition(i) + "; offset " + getOffset(i) + "; block length " + getLen(i));
    }
  }

  /**
   * Get the total number of blocks in the index file.
   */
  public int getNumberOfBlocks() {
    return blockPositions_.length;
  }

  /**
   * Get the block offset for a given block.
   * @param block
   * @return the byte offset into the file where this block starts.  It is the developer's
   * responsibility to call getNumberOfBlocks() to know appropriate bounds on the parameter.
   * The argument block should satisfy 0 <= block < getNumberOfBlocks().
   */
  public long getPosition(int block) {
    return blockPositions_[block];
  }

  /**
   * Get the file stream offset for a given block.
   * @param block
   * @return the uncompressed byte offset where this block starts.  It is the developer's
   * responsibility to call getNumberOfBlocks() to know appropriate bounds on the parameter.
   * The argument block should satisfy 0 <= block < getNumberOfBlocks().
   */
  public long getOffset(int block) {
    return streamOffsets_[block];
  }

  /**
   * Get the number of uncompressed bytes in the given block.
   * @param block
   * @return the number of uncompressed bytes in this block.  It is the developer's
   * responsibility to call getNumberOfBlocks() to know appropriate bounds on the parameter.
   * The argument block should satisfy 0 <= block < getNumberOfBlocks().
   */
  public long getLen(int block) {
    return uncBlockLengths_[block];
  }

  /**
   * Find the next lzo block start from the given position.
   *
   * @param pos The position to start looking from.
   * @return The block or -1 if it couldn't be found.
   */
  public int findNextBlock(long pos) {
    int block = Arrays.binarySearch(blockPositions_, pos);

    if (block >= 0) {
      // direct hit on a block start position
      return block;
    } else {
      block = Math.abs(block) - 1;
      if (block > blockPositions_.length - 1) {
        return (int) NOT_FOUND;
      }
      return block;
    }
  }

  /**
   * Return true if the index has no blocks set.
   *
   * @return true if the index has no blocks set.
   */
  public boolean isEmpty() {
    return blockPositions_ == null || blockPositions_.length == 0;
  }

  /**
   * Nudge a given file slice start to the nearest LZO block start no earlier than
   * the current slice start.
   *
   * @param start The current slice start
   * @param end The current slice end
   * @return The smallest block id with file offset between [start, end), or
   *         NOT_FOUND if there is none such.
   */
  public int alignSliceStartToBlock(long start, long end) {
    int block = 0;
    if (start != 0) {
      // find the next block position from
      // the start of the split
      block = findNextBlock(start);
      if (block == (int) NOT_FOUND) {
        return (int) NOT_FOUND;
      }

      long newStart = getPosition(block);
      if (newStart >= end) {
        return (int) NOT_FOUND;
      }
    }
    // if start is 0, then we assume we want block 0
    return block;
  }

  /**
   * Nudge a given file slice end to the nearest LZO block end no earlier than
   * the current slice end.
   *
   * @param end The current slice end
   * @param fileSize The size of the file, i.e. the max end position.
   * @return The smallest block id with an index between [end, fileSize].
   *         If there is not another block, return NOT_FOUND (or -1).
   */
  public int alignSliceEndToBlock(long end) {
    return findNextBlock(end);
  }

  /**
   * Read the index of the lzo file.

   * @param fs The index file is on this file system.
   * @param lzoFile the file whose index we are reading -- NOT the index file itself.  That is,
   * pass in filename.lzo, not filename.lzo.index, for this parameter.
   * @throws IOException
   */
  public static LzoIndex readIndex(FileSystem fs, Path lzoFile) throws IOException {
    FSDataInputStream indexIn = null;
    try {
      Path indexFile = lzoFile.suffix(LZO_INDEX_SUFFIX);
      if (!fs.exists(indexFile)) {
        // return empty index, fall back to the unsplittable mode
        return new LzoIndex();
      }

      long indexLen = fs.getFileStatus(indexFile).getLen();
      int blocks = (int) (indexLen / (8 + 8 + 8));
      LzoIndex index = new LzoIndex(blocks);
      indexIn = fs.open(indexFile);
      for (int i = 0; i < blocks; i++) {
        index.set(i, indexIn.readLong(), indexIn.readLong(), indexIn.readLong());
      }
      return index;
    } finally {
      if (indexIn != null) {
        indexIn.close();
      }
    }
  }

  /**
   * Index an lzo file to allow the input format to split them into separate map
   * jobs.
   *
   * @param fs File system that contains the file.
   * @param lzoFile the lzo file to index.  For filename.lzo, the created index file will be
   * filename.lzo.index.
   * @throws IOException
   */
  public static void createIndex(FileSystem fs, Path lzoFile)
  throws IOException {

    Configuration conf = fs.getConf();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(lzoFile);
    if (null == codec) {
      throw new IOException("Could not find codec for file " + lzoFile +
        " - you may need to add the LZO codec to your io.compression.codecs " +
        "configuration in core-site.xml");
    }
    ((Configurable) codec).setConf(conf);

    FSDataInputStream is = null;
    FSDataOutputStream os = null;
    Path outputFile = lzoFile.suffix(LZO_INDEX_SUFFIX);
    Path tmpOutputFile = lzoFile.suffix(LZO_TMP_INDEX_SUFFIX);

    // Track whether an exception was thrown or not, so we know to either
    // delete the tmp index file on failure, or rename it to the new index file on success.
    boolean indexingSucceeded = false;
    try {
      is = fs.open(lzoFile);
      os = fs.create(tmpOutputFile);
      LzopDecompressor decompressor = (LzopDecompressor) codec.createDecompressor();
      // Solely for reading the header
      codec.createInputStream(is, decompressor);
      int numCompressedChecksums = decompressor.getCompressedChecksumsCount();
      int numDecompressedChecksums = decompressor.getDecompressedChecksumsCount();

      long offset = 0;

      while (true) {
        LOG.info("reading new compressed block. file pos is " + is.getPos());

        // read and ignore, we just want to get to the next int
        int uncompressedBlockSize = is.readInt();
        if (uncompressedBlockSize == 0) {
          break;
        } else if (uncompressedBlockSize < 0) {
          throw new EOFException();
        }

        int compressedBlockSize = is.readInt();
        if (compressedBlockSize <= 0) {
          throw new IOException("Could not read compressed block size");
        }

        // See LzopInputStream.getCompressedData
        boolean isUncompressedBlock = (uncompressedBlockSize == compressedBlockSize);
        int numChecksumsToSkip = isUncompressedBlock ?
            numDecompressedChecksums : numDecompressedChecksums + numCompressedChecksums;
        long pos = is.getPos();
        // write the pos of the block start
        os.writeLong(pos - 8);

        // write the byte stream offset at this block 
        os.writeLong(offset);

        // write the number of uncompressed bytes in this block
        os.writeLong(uncompressedBlockSize);

        LOG.info("block: uncSize " + uncompressedBlockSize + "; cSize " + compressedBlockSize + "; pos " + pos + "; block start " + (pos - 8) + "; offset " + offset);

        // update the current file offset
        offset += uncompressedBlockSize;

        // seek to the start of the next block, skip any checksums
        long seekto = pos + compressedBlockSize + (4 * numChecksumsToSkip);
        LOG.info("seeking to " + pos + " + " + compressedBlockSize + " + 4*" + numChecksumsToSkip + " = " + seekto);

        is.seek(seekto);
      }
      // If we're here, indexing was successful.
      indexingSucceeded = true;
    } finally {
      // Close any open streams.
      if (is != null) {
        is.close();
      }

      if (os != null) {
        os.close();
      }

      if (!indexingSucceeded) {
        // If indexing didn't succeed (i.e. an exception was thrown), clean up after ourselves.
        fs.delete(tmpOutputFile, false);
      } else {
        // Otherwise, rename filename.lzo.index.tmp to filename.lzo.index.
        fs.rename(tmpOutputFile, outputFile);
      }
    }
  }
}

