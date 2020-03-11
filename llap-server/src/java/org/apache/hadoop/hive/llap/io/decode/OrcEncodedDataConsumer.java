/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.decode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.ConsumerFeedback;
import org.apache.hadoop.hive.llap.counters.LlapIOCounters;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer.Includes;
import org.apache.hadoop.hive.llap.io.metadata.ConsumerFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.ConsumerStripeMetadata;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.BloomKFilter;
import org.apache.orc.CompressionCodec;
import org.apache.orc.OrcProto.CalendarKind;
import org.apache.orc.impl.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedTreeReaderFactory;
import org.apache.hadoop.hive.ql.io.orc.encoded.EncodedTreeReaderFactory.SettableTreeReader;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.encoded.Reader.OrcEncodedColumnBatch;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.TreeReaderFactory.StructTreeReader;
import org.apache.orc.impl.TreeReaderFactory.TreeReader;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.OrcProto;


public class OrcEncodedDataConsumer
  extends EncodedDataConsumer<OrcBatchKey, OrcEncodedColumnBatch> {
  private TreeReaderFactory.TreeReader[] columnReaders;
  private int previousStripeIndex = -1;
  private ConsumerFileMetadata fileMetadata; // We assume one request is only for one file.
  private CompressionCodec codec;
  private List<ConsumerStripeMetadata> stripes;
  private final boolean skipCorrupt; // TODO: get rid of this
  private SchemaEvolution evolution;
  private IoTrace trace;
  private final Includes includes;
  private TypeDescription[] batchSchemas;
  private boolean useDecimal64ColumnVectors;
  // ProbeDecode extra variables
  private boolean probeDecodeEnabled = false;
  private boolean probeDecodeCuckooEnabled = false;
  private float probeDecodeFilterThresPerc;
  private BloomKFilter probeDecodeMapBf = null;
  private VectorMapJoinHashTable probeDecodeMapJoinTable = null;
  private ObjectCache probeDecodeCache = null;


  public OrcEncodedDataConsumer(Consumer<ColumnVectorBatchWrapper> consumer, Includes includes, boolean skipCorrupt,
      QueryFragmentCounters counters, LlapDaemonIOMetrics ioMetrics, Configuration conf) {
    super(consumer, includes.getPhysicalColumnIds().size(), ioMetrics, counters);
    this.includes = includes;
    // TODO: get rid of this
    this.skipCorrupt = skipCorrupt;
    this.probeDecodeEnabled = HiveConf.getBoolVar(includes.getJobConf(), HiveConf.ConfVars.HIVE_MAPJOIN_PROBEDECODE_ENABLED) &&
        this.includes.getProbeDecodeCacheKey() != null;
    this.probeDecodeCuckooEnabled = HiveConf.getBoolVar(includes.getJobConf(), HiveConf.ConfVars.HIVE_MAPJOIN_PROBEDECODE_BF_ENABLED);
    this.probeDecodeFilterThresPerc = HiveConf.getFloatVar(includes.getJobConf(), HiveConf.ConfVars.HIVE_MAPJOIN_PROBEDECODE_FILTER_PERC);
    if (this.probeDecodeEnabled) {
      this.probeDecodeCache = ObjectCacheFactory.getCache(conf, HiveConf.getVar(includes.getJobConf(), HiveConf.ConfVars.HIVEQUERYID), false);
      // Start periodic MapJoinTable checker
      ExecutorService executorService = Executors.newFixedThreadPool(1);
      executorService.execute(()->{
        while (probeDecodeMapJoinTable == null && probeDecodeMapBf == null) {
          try {
            // TODO: Check if we can get directly from operator
            Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]> smallTablePair = probeDecodeCache.retrieve(includes.getProbeDecodeCacheKey());
            if (smallTablePair != null ) {
              VectorMapJoinHashTable ht = ((VectorMapJoinTableContainer) smallTablePair.getLeft()[includes.getPosSingleVectorMapJoinSmallTable()]).vectorMapJoinHashTable();
              if (probeDecodeCuckooEnabled)
                this.probeDecodeMapBf = ht.getHashTableKeys();
              else
                this.probeDecodeMapJoinTable = ht;
              LlapIoImpl.LOG.info("ProbeDecode finally got HashTable of size {}", ht.size());
            } else {
              Thread.sleep(50);
            }
          } catch (HiveException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }
    LlapIoImpl.LOG.info("OrcEncodedDataConsumer probeDecode enabled is {} with cacheKey {} colIndex {} and colName {}",
        probeDecodeEnabled, this.includes.getProbeDecodeCacheKey(), this.includes.getProbeDecodeColIdx(), this.includes.getProbeDecodeColName());
  }

  public OrcEncodedDataConsumer(
    Consumer<ColumnVectorBatchWrapper> consumer, Includes includes, boolean skipCorrupt,
    QueryFragmentCounters counters, LlapDaemonIOMetrics ioMetrics) {
    super(consumer, includes.getPhysicalColumnIds().size(), ioMetrics, counters);
    this.includes = includes;
    // TODO: get rid of this
    this.skipCorrupt = skipCorrupt;
  }

  public void setUseDecimal64ColumnVectors(final boolean useDecimal64ColumnVectors) {
    this.useDecimal64ColumnVectors = useDecimal64ColumnVectors;
  }

  public void setFileMetadata(ConsumerFileMetadata f) {
    assert fileMetadata == null;
    fileMetadata = f;
    stripes = new ArrayList<>(f.getStripeCount());
    codec = WriterImpl.createCodec(fileMetadata.getCompressionKind());
  }

  public void setStripeMetadata(ConsumerStripeMetadata m) {
    assert stripes != null;
    int newIx = m.getStripeIx();
    for (int i = stripes.size(); i <= newIx; ++i) {
      stripes.add(null);
    }
    assert stripes.get(newIx) == null;
    stripes.set(newIx, m);
  }

  @Override
  protected void decodeBatch(OrcEncodedColumnBatch batch,
      Consumer<ColumnVectorBatchWrapper> downstreamConsumer) throws InterruptedException {
    long startTime = counters.startTimeCounter();
    int currentStripeIndex = batch.getBatchKey().stripeIx;

    boolean sameStripe = currentStripeIndex == previousStripeIndex;

    try {
      ConsumerStripeMetadata stripeMetadata = stripes.get(currentStripeIndex);
      // Get non null row count from root column, to get max vector batches
      int rgIdx = batch.getBatchKey().rgIx;
      long nonNullRowCount;
      long nonSkippedRowCount = 0;
      boolean noIndex = false;
      if (rgIdx == OrcEncodedColumnBatch.ALL_RGS) {
        nonNullRowCount = stripeMetadata.getRowCount();
      } else {
        OrcProto.RowIndexEntry rowIndex = stripeMetadata.getRowIndexEntry(0, rgIdx);
        // index is disabled
        if (rowIndex == null) {
          nonNullRowCount = stripeMetadata.getRowCount();
          noIndex = true;
        } else {
          nonNullRowCount = getRowCount(rowIndex);
        }
      }
      int maxBatchesRG = (int) ((nonNullRowCount / VectorizedRowBatch.DEFAULT_SIZE) + 1);
      int batchSize = VectorizedRowBatch.DEFAULT_SIZE;
      TypeDescription fileSchema = fileMetadata.getSchema();

      if (columnReaders == null || !sameStripe || noIndex) {
        createColumnReaders(batch, stripeMetadata, fileSchema);
      } else {
        repositionInStreams(this.columnReaders, batch, sameStripe, stripeMetadata);
      }
      previousStripeIndex = currentStripeIndex;

//       if (this.probeDecodeEnabled && columnReaders.length == 20) {
//         LlapIoImpl.LOG.info("PANOS HERE Waiting...");
//         Thread.sleep(300);
//         LlapIoImpl.LOG.info("PANOS HERE Waiting...DONE!");
//       }

      // For every row group
      for (int i = 0; i < maxBatchesRG; i++) {
        // for last batch in row group, adjust the batch size
        if (i == maxBatchesRG - 1) {
          batchSize = (int) (nonNullRowCount % VectorizedRowBatch.DEFAULT_SIZE);
          if (batchSize == 0) break;
        }

        ColumnVectorBatchWrapper cvbw = cvbPool.take();
        cvbw.getFilterContext().resetFilterContext();
        // assert cvb.cols.length == batch.getColumnIxs().length; // Must be constant per split.
        cvbw.getCvb().size = batchSize;
        // for every column of the row group
        for (int idx = 0; idx < columnReaders.length; ++idx) {
          TreeReader reader = columnReaders[idx];
          if (cvbw.getCvb().cols[idx] == null) {
            // Orc store rows inside a root struct (hive writes it this way).
            // When we populate column vectors we skip over the root struct.
            cvbw.getCvb().cols[idx] = createColumn(batchSchemas[idx], VectorizedRowBatch.DEFAULT_SIZE, useDecimal64ColumnVectors);
          }
          trace.logTreeReaderNextVector(idx);

          /*
           * Currently, ORC's TreeReaderFactory class does this:
           *
           *     public void nextBatch(VectorizedRowBatch batch,
           *              int batchSize) throws IOException {
           *       batch.cols[0].reset();
           *       batch.cols[0].ensureSize(batchSize, false);
           *       nextVector(batch.cols[0], null, batchSize);
           *     }
           *
           * CONCERN:
           *     For better performance, we'd like to *not* do a ColumnVector.reset()
           *     which zeroes out isNull.  Why?  Because there are common cases where
           *     ORC will *immediately* copy its null flags into the isNull array.  This is a
           *     waste.
           *
           *     For correctness now we must do it for now.
           *
           *     The best solution is for ORC to manage the noNulls and isNull array itself
           *     because it knows what NULLs the next set of rows contains.
           *
           *     Its management of the fields of ColumnVector is a little different than what we
           *     must do for vector expressions.  For those, we must maintain the invariant that if
           *     noNulls is true there are no NULLs in any part of the isNull array.  This is
           *     because the next vector expression relies on the invariant.
           *
           *     Given that ORC (or any other producer) is providing *read-only* batches to the
           *     consumer, what is important is that the isNull array through batch.size has
           *     integrity with the noNulls flag.  So, if ORC is giving us 100 rows (for example)
           *     and none of them are NULL, it can safely set or make sure the first 100 isNull
           *     entries are false and safely set noNulls to true.  Any other NULLs (true entries)
           *     in isNull are irrelevant because ORC owns the batch.  It just need to make sure
           *     it doesn't get confused.
           *
           */
          ColumnVector cv = cvbw.getCvb().cols[idx];
          cv.reset();
          cv.ensureSize(batchSize, false);
          // if this is the probeKey Col and all rows are skipped -> skip decoding for the remaining batch columns
          if ((idx == this.includes.getProbeDecodeColIdx())) {
            reader.nextVector(cv, null, batchSize);
            if (probeDecodeMapJoinTable != null)
              HashTableRowFilterCallback(cvbw, idx, batchSize);
          } else {
            reader.setFilterContext(cvbw.getFilterContext().immutable());
            reader.nextVector(cv, null, batchSize);
          }
        }
        if (cvbw.getCvb().size > 0) {
          downstreamConsumer.consumeData(cvbw);
          counters.incrCounter(LlapIOCounters.ROWS_EMITTED, batchSize);
        }
        nonSkippedRowCount += cvbw.getFilterContext().getSelectedSize();
      }
      LlapIoImpl.ORC_LOGGER.debug("Done with decode, skippedRows {} out of {} on {} Cols", nonNullRowCount - nonSkippedRowCount,
          nonNullRowCount, columnReaders.length);
      counters.incrCounter(LlapIOCounters.NUM_DECODED_ROWS, nonSkippedRowCount);
      counters.incrWallClockCounter(LlapIOCounters.DECODE_TIME_NS, startTime);
      counters.incrCounter(LlapIOCounters.NUM_VECTOR_BATCHES, maxBatchesRG);
      counters.incrCounter(LlapIOCounters.NUM_DECODED_BATCHES);
    } catch (IOException e) {
      // Caller will return the batch.
      downstreamConsumer.setError(e);
    }
  }

  // Filter out rows in a round-robbin fashion starting with a pass
  public void HashTableRowFilterCallback(ColumnVectorBatchWrapper cvbw, int colIdx, int batchSize) {
    long startDecodeTime = counters.startTimeCounter();
    TreeReaderFactory.MutableFilterContext cntx = cvbw.getFilterContext();
    int[] selected = null;
    int newSize = 0;
    boolean selectedInUse = false;
    if (cvbw.getCvb().cols[colIdx] instanceof LongColumnVector) {
      LongColumnVector probeCol = (LongColumnVector) cvbw.getCvb().cols[colIdx];
      // NoNulls case
      if (probeCol.isRepeating) {
        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        if (probeDecodeCuckooEnabled) {
          if (!(probeDecodeMapBf.testLong(probeCol.vector[0]))) {
            // Entire batch is filtered out.
            cvbw.getCvb().size = 0;
          }
        } else {
          if (!(probeDecodeMapJoinTable.containsLongKey(probeCol.vector[0]))) {
            // Entire batch is filtered out.
            cvbw.getCvb().size = 0;
          }
        }
      } else {
        selected = cntx.borrowSelected(batchSize);
        for (int row = 0; row < batchSize; ++row) {
          // Pass ony Valid key
          if (probeDecodeCuckooEnabled) {
            if (probeDecodeMapBf.testLong(probeCol.vector[row])) {
              selected[newSize++] = row;
            }
          } else {
            if (probeDecodeMapJoinTable.containsLongKey(probeCol.vector[row])) {
              selected[newSize++] = row;
            }
          }
        }
        if (newSize == 0) cvbw.getCvb().size = 0;
        // skip only if <= Threshold % match
        if (newSize <= (probeDecodeFilterThresPerc * batchSize)) {
          selectedInUse = true;
        }
      }
    }
//    LlapIoImpl.ORC_LOGGER.info("PANOS Matched: {} selectedInUse {} batchSize {} newCV size: {}", newSize, selectedInUse, batchSize, cvbw.getCvb().size);
    cntx.setFilterContext(selectedInUse, selected, newSize);
    counters.incrWallClockCounter(LlapIOCounters.PROBE_DECODE_TIME_NS, startDecodeTime);
  }

  private void createColumnReaders(OrcEncodedColumnBatch batch,
      ConsumerStripeMetadata stripeMetadata, TypeDescription fileSchema) throws IOException {
    TreeReaderFactory.Context context = new TreeReaderFactory.ReaderContext()
            .setSchemaEvolution(evolution).skipCorrupt(skipCorrupt)
            .writerTimeZone(stripeMetadata.getWriterTimezone())
            .fileFormat(fileMetadata == null ? null : fileMetadata.getFileVersion())
            .useUTCTimestamp(true)
            .setProlepticGregorian(fileMetadata != null && fileMetadata.getCalendar() == CalendarKind.PROLEPTIC_GREGORIAN, true);
    this.batchSchemas = includes.getBatchReaderTypes(fileSchema);
    StructTreeReader treeReader = EncodedTreeReaderFactory.createRootTreeReader(
        batchSchemas, stripeMetadata.getEncodings(), batch, codec, context, useDecimal64ColumnVectors);
    this.columnReaders = treeReader.getChildReaders();

    if (LlapIoImpl.LOG.isDebugEnabled()) {
      for (int i = 0; i < columnReaders.length; ++i) {
        LlapIoImpl.LOG.debug("Created a reader at " + i + ": " + columnReaders[i]
            + " from schema " + batchSchemas[i]);
      }
    }
    positionInStreams(columnReaders, batch.getBatchKey(), stripeMetadata);
  }

  private ColumnVector createColumn(TypeDescription type, int batchSize, final boolean useDecimal64ColumnVectors) {
    switch (type.getCategory()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongColumnVector(batchSize);
      case DATE:
        return new DateColumnVector(batchSize);
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector(batchSize);
      case BINARY:
      case STRING:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector(batchSize);
      case TIMESTAMP:
        return new TimestampColumnVector(batchSize);
      case DECIMAL:
        if (useDecimal64ColumnVectors && type.getPrecision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
          return new Decimal64ColumnVector(batchSize, type.getPrecision(), type.getScale());
        } else {
          return new DecimalColumnVector(batchSize, type.getPrecision(), type.getScale());
        }
      case STRUCT: {
        List<TypeDescription> subtypeIdxs = type.getChildren();
        ColumnVector[] fieldVector = new ColumnVector[subtypeIdxs.size()];
        for (int i = 0; i < fieldVector.length; ++i) {
          fieldVector[i] = createColumn(subtypeIdxs.get(i), batchSize, useDecimal64ColumnVectors);
        }
        return new StructColumnVector(batchSize, fieldVector);
      }
      case UNION: {
        List<TypeDescription> subtypeIdxs = type.getChildren();
        ColumnVector[] fieldVector = new ColumnVector[subtypeIdxs.size()];
        for (int i = 0; i < fieldVector.length; ++i) {
          fieldVector[i] = createColumn(subtypeIdxs.get(i), batchSize, useDecimal64ColumnVectors);
        }
        return new UnionColumnVector(batchSize, fieldVector);
      }
      case LIST:
        return new ListColumnVector(batchSize, createColumn(type.getChildren().get(0), batchSize,
          useDecimal64ColumnVectors));
      case MAP:
        List<TypeDescription> subtypeIdxs = type.getChildren();
        return new MapColumnVector(batchSize, createColumn(subtypeIdxs.get(0), batchSize, useDecimal64ColumnVectors),
          createColumn(subtypeIdxs.get(1), batchSize, useDecimal64ColumnVectors));
      default:
        throw new IllegalArgumentException("LLAP does not support " + type.getCategory());
    }
  }

  private void positionInStreams(TreeReaderFactory.TreeReader[] columnReaders,
      OrcBatchKey batchKey, ConsumerStripeMetadata stripeMetadata) throws IOException {
    PositionProvider[] pps = createPositionProviders(columnReaders, batchKey, stripeMetadata);
    if (pps == null) return;
    for (int i = 0; i < columnReaders.length; i++) {
      if (columnReaders[i] == null) continue;
      // TODO: we could/should trace seek destinations; pps needs a "peek" method
      columnReaders[i].seek(pps);
    }
  }

  private void repositionInStreams(TreeReaderFactory.TreeReader[] columnReaders,
      EncodedColumnBatch<OrcBatchKey> batch, boolean sameStripe,
      ConsumerStripeMetadata stripeMetadata) throws IOException {
    PositionProvider[] pps = createPositionProviders(
        columnReaders, batch.getBatchKey(), stripeMetadata);
    if (LlapIoImpl.ORC_LOGGER.isTraceEnabled()) {
      LlapIoImpl.ORC_LOGGER.trace("Created pps {}", Arrays.toString(pps));
    }
    if (pps == null) return;
    for (int i = 0; i < columnReaders.length; i++) {
      TreeReader reader = columnReaders[i];
      if (reader == null) continue;
      // Note: we assume this never happens for SerDe reader - the batch would never have vectors.
      // That is always true now; but it wasn't some day, the below would throw in getColumnData.
      ((SettableTreeReader) reader).setBuffers(batch, sameStripe);
      // TODO: When hive moves to java8, make updateTimezone() as default method in
      // SettableTreeReader so that we can avoid this check.
      if (reader instanceof EncodedTreeReaderFactory.TimestampStreamReader && !sameStripe) {
        ((EncodedTreeReaderFactory.TimestampStreamReader) reader)
                .updateTimezone(stripeMetadata.getWriterTimezone());
      }
      reader.seek(pps);
    }
  }

  /**
   * Position provider used in absence of indexes, e.g. for serde-based reader, where each stream
   * is in its own physical 'container', always starting at 0, and there are no RGs.
   */
  private final static class IndexlessPositionProvider implements PositionProvider {
    @Override
    public long getNext() {
      return 0;
    }

    @Override
    public String toString() {
      return "indexes not supported";
    }
  }

  private PositionProvider[] createPositionProviders(
      TreeReaderFactory.TreeReader[] columnReaders, OrcBatchKey batchKey,
      ConsumerStripeMetadata stripeMetadata) throws IOException {
    if (columnReaders.length == 0) return null;
    PositionProvider[] pps = null;
    if (!stripeMetadata.supportsRowIndexes()) {
      PositionProvider singleRgPp = new IndexlessPositionProvider();
      pps = new PositionProvider[stripeMetadata.getEncodings().size()];
      for (int i = 0; i < pps.length; ++i) {
        pps[i] = singleRgPp;
      }
    } else {
      int rowGroupIndex = batchKey.rgIx;
      if (rowGroupIndex == OrcEncodedColumnBatch.ALL_RGS) {
        throw new IOException("Cannot position readers without RG information");
      }
      // TODO: this assumes indexes in getRowIndexes would match column IDs
      OrcProto.RowIndex[] ris = stripeMetadata.getRowIndexes();
      pps = new PositionProvider[ris.length];
      for (int i = 0; i < ris.length; ++i) {
        OrcProto.RowIndex ri = ris[i];
        if (ri == null) continue;
        pps[i] = new RecordReaderImpl.PositionProviderImpl(ri.getEntry(rowGroupIndex));
      }
    }
    return pps;
  }

  private long getRowCount(OrcProto.RowIndexEntry rowIndexEntry) {
    return rowIndexEntry.getStatistics().getNumberOfValues();
  }

  public void setSchemaEvolution(SchemaEvolution evolution) {
    this.evolution = evolution;
  }

  public SchemaEvolution getSchemaEvolution() {
    return evolution;
  }

  public void init(ConsumerFeedback<OrcEncodedColumnBatch> upstreamFeedback,
      Callable<Void> readCallable, IoTrace trace) {
    super.init(upstreamFeedback, readCallable);
    this.trace = trace;
  }
}
