/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.DatasetOutputCommitter;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.Partition;
import co.cask.cdap.api.dataset.lib.PartitionConsumerResult;
import co.cask.cdap.api.dataset.lib.PartitionConsumerState;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionMetadata;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.Partitioning.FieldType;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Provider;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Implementation of partitioned datasets using a Table to store the meta data.
 */
public class PartitionedFileSetDataset extends AbstractDataset implements PartitionedFileSet, DatasetOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetDataset.class);

  // column keys
  protected static final byte[] RELATIVE_PATH = { 'p' };
  protected static final byte[] FIELD_PREFIX = { 'f', '.' };
  protected static final byte[] METADATA_PREFIX = { 'm', '.' };
  protected static final byte[] CREATION_TIME = { 'c' };

  protected final FileSet files;
  protected final IndexedTable partitionsTable;
  protected final Map<String, String> runtimeArguments;
  protected final DatasetSpecification spec;
  protected final Provider<ExploreFacade> exploreFacadeProvider;
  protected final Partitioning partitioning;
  protected boolean ignoreInvalidRowsSilently = false;

  private final Id.DatasetInstance datasetInstanceId;

  // In this map we keep track of the partitions that were added in the same transaction.
  // If the exact same partition is added again, we will not throw an error but only log a message that
  // the partition already exists. The reason for this is to provide backward-compatibility for CDAP-1227:
  // by adding the partition in the onSuccess() of this dataset, map/reduce programs do not need to do that in
  // their onFinish() any longer. But existing map/reduce programs may still do that, and would now fail.
  private final Map<String, PartitionKey> partitionsAddedInSameTx = Maps.newHashMap();
  private Transaction tx;

  public PartitionedFileSetDataset(DatasetContext datasetContext, String name,
                                   Partitioning partitioning, FileSet fileSet, IndexedTable partitionTable,
                                   DatasetSpecification spec, Map<String, String> arguments,
                                   Provider<ExploreFacade> exploreFacadeProvider) {
    super(name, partitionTable);
    this.files = fileSet;
    this.partitionsTable = partitionTable;
    this.spec = spec;
    this.exploreFacadeProvider = exploreFacadeProvider;
    this.runtimeArguments = arguments;
    this.partitioning = partitioning;
    this.datasetInstanceId = Id.DatasetInstance.from(datasetContext.getNamespaceId(), name);
  }

  @Override
  public void startTx(Transaction tx) {
    partitionsAddedInSameTx.clear();
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public boolean commitTx() throws Exception {
    this.tx = null;
    return super.commitTx();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    this.tx = null;
    return super.rollbackTx();
  }

  @Override
  public Partitioning getPartitioning() {
    return partitioning;
  }

  @Override
  public void addPartition(PartitionKey key, String path) {
    addPartition(key, path, Collections.<String, String>emptyMap());
  }

  @Override
  public void addPartition(PartitionKey key, String path, Map<String, String> metadata) {
    addPartition(key, path, true, metadata);
  }

  /**
   * Add a partition for a given partition key, stored at a given path (relative to the file set's base path),
   * with the given metadata.
   *
   * @param key the partitionKey for the partition.
   * @param path the path for the partition.
   * @param explorable whether to add the partition to explore
   * @param metadata the metadata associated with the partition.
   */
  protected void addPartition(PartitionKey key, String path, boolean explorable, Map<String, String> metadata) {
    byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    if (!row.isEmpty()) {
      if (key.equals(partitionsAddedInSameTx.get(path))) {
        LOG.warn("Dataset {} already added a partition with key {} in this transaction. " +
                   "Partitions no longer need to be added in the onFinish() of MapReduce. Please check your app. ",
                 getName(), key.toString());
        return;
      }
      throw new DataSetException(String.format("Dataset '%s' already has a partition with the same key: %s",
                                               getName(), key.toString()));
    }
    LOG.debug("Adding partition with key {} and path {} to dataset {}", key, path, getName());
    Put put = new Put(rowKey);
    put.add(RELATIVE_PATH, Bytes.toBytes(path));
    byte[] nowInMillis = Bytes.toBytes(System.currentTimeMillis());
    put.add(CREATION_TIME, nowInMillis);
    for (Map.Entry<String, ? extends Comparable> entry : key.getFields().entrySet()) {
      put.add(Bytes.add(FIELD_PREFIX, Bytes.toBytes(entry.getKey())), // "f.<field name>"
              Bytes.toBytes(entry.getValue().toString()));            // "<string rep. of value>"
    }

    addMetadataToPut(metadata, put);
    // index each row by its transaction's read pointer
    put.add(PartitionedFileSetDefinition.INDEX_COLUMN, tx.getWritePointer());

    partitionsTable.put(put);
    partitionsAddedInSameTx.put(path, key);


    if (explorable) {
      addPartitionToExplore(key, path);
      // TODO: make DDL operations transactional [CDAP-1393]
    }
  }

  @Override
  public PartitionConsumerResult consumePartitions(PartitionConsumerState partitionConsumerState) {
    long[] previousInProgress = partitionConsumerState.getVersionsToCheck();
    long[] inProgress = tx.getInProgress();
    List<Long> noLongerInProgress = getDiff(previousInProgress, inProgress);

    List<PartitionIterator> partitionIterators = Lists.newArrayList();
    for (long version : noLongerInProgress) {
      // TODO: maybe scan the entire range of noLongerInProgress, instead of each being a lookup (for performance)
      Scanner scanner = partitionsTable.readByIndex(PartitionedFileSetDefinition.INDEX_COLUMN,
                                                    Bytes.toBytes(version));
      partitionIterators.add(new PartitionIterator(scanner,
                                                   null));
//                                                   noLongerInProgress.toArray(new Long[noLongerInProgress.size()])));
    }

    Long startVersion = partitionConsumerState.getStartVersion();
    Scanner scanner = partitionsTable.scanByIndex(PartitionedFileSetDefinition.INDEX_COLUMN,
                                                  startVersion == null ? null : Bytes.toBytes(startVersion),
                                                  null);
    partitionIterators.add(new PartitionIterator(scanner, null));
    // This (using the write pointer as the startVersion) can be problematic for consuming partitioned added within the
    // same transaction.
    // For instance, if we add a partition and then consume partitions, then the next partition consumption will also
    // include the same partition. We can work around this problem by using tx.getWritePointer + 1 as the beginning of
    // the next scan, but this causes a similar problem in the case that we consume partitions first, and then add the
    // partition (it might be excluded).
    return new PartitionConsumerResult(new PartitionConsumerState(tx.getWritePointer(), inProgress),
                                       Iterators.concat(partitionIterators.iterator()));
  }

  // Assumptions made: both input arrays are sorted & have unique values
  // returns the list of longs that are contained within oldLongs, but not newLongs (oldLongs - newLongs)
  public static List<Long> getDiff(long[] oldLongs, long[] newLongs) {
    List<Long> diff = Lists.newArrayList();

    int newIdx = 0;
    // for every long in oldLongs, add it to the diff unless it is found in newLongs
    for (long oldLong : oldLongs) {
      // TODO: could break out once newIdx >= newLongs.length for efficiency
      while (newIdx < newLongs.length && newLongs[newIdx] < oldLong) {
        newIdx++;
      }
      // if it exists in newLongs, continue;
      if (newIdx < newLongs.length && newLongs[newIdx] == oldLong) {
        newIdx++;
        continue;
      }
      diff.add(oldLong);
    }
    return diff;
  }

  private final class PartitionIterator implements Iterator<Partition> {
    private final Long[] versionsToCheck;
    private final Scanner scanner;
    private Partition partition;

    public PartitionIterator(Scanner scanner, @Nullable Long[] versionsToCheck) {
      this.scanner = scanner;
      this.versionsToCheck = versionsToCheck;
      this.partition = getNextPartition();
    }

    @Override
    public boolean hasNext() {
      return partition != null;
    }

    @Override
    public Partition next() {
      if (partition == null) {
        throw new NoSuchElementException();
      }
      Partition partitionToReturn = partition;
      partition = getNextPartition();
      return partitionToReturn;
    }

    private Partition getNextPartition() {
      Row row = scanner.next();
      if (versionsToCheck != null) {
        // we exclude all the values that are not within the specified set of values (versionsToCheck)
        while (row != null &&
          0 > Arrays.binarySearch(versionsToCheck, Bytes.toLong(row.get(PartitionedFileSetDefinition.INDEX_COLUMN)))) {
          row = scanner.next();
        }
      }
      return rowToPartition(row);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private Partition rowToPartition(@Nullable Row row) {
      if (row == null) {
        return null;
      }
      PartitionKey key = parseRowKey(row.getRow(), partitioning);
      String relativePath = Bytes.toString(row.get(RELATIVE_PATH));
      return new BasicPartitionDetail(PartitionedFileSetDataset.this, relativePath, key, metadataFromRow(row));
    }
  }

  @Override
  public void addMetadata(PartitionKey key, String metadataKey, String metadataValue) {
    addMetadata(key, ImmutableMap.of(metadataKey, metadataValue));
  }

  @Override
  public void addMetadata(PartitionKey key, Map<String, String> metadata) {
    final byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    if (row.isEmpty()) {
      throw new DataSetException(String.format("Dataset '%s' does not have a partition for key: %s", getName(), key));
    }

    // ensure that none of the entries already exist in the metadata
    for (Map.Entry<String, String> metadataEntry : metadata.entrySet()) {
      String metadataKey = metadataEntry.getKey();
      byte[] columnKey = columnKeyFromMetadataKey(metadataKey);
      if (row.get(columnKey) != null) {
        throw new DataSetException(String.format("Entry already exists for metadata key: %s", metadataKey));
      }
    }

    Put put = new Put(rowKey);
    addMetadataToPut(metadata, put);
    partitionsTable.put(put);
  }

  private void addMetadataToPut(Map<String, String> metadata, Put put) {
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      byte[] columnKey = columnKeyFromMetadataKey(entry.getKey());
      put.add(columnKey, Bytes.toBytes(entry.getValue()));
    }
  }

  protected void addPartitionToExplore(PartitionKey key, String path) {
    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.addPartition(datasetInstanceId, key, files.getLocation(path).toURI().getPath());
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to add partition for key %s with path %s to explore table.", key.toString(), path), e);
        }
      }
    }
  }

  @Override
  public void dropPartition(PartitionKey key) {
    byte[] rowKey = generateRowKey(key, partitioning);
    partitionsTable.delete(rowKey);
    dropPartitionFromExplore(key);
    // TODO: make DDL operations transactional [CDAP-1393]
  }

  private void dropPartitionFromExplore(PartitionKey key) {
    if (FileSetProperties.isExploreEnabled(spec.getProperties())) {
      ExploreFacade exploreFacade = exploreFacadeProvider.get();
      if (exploreFacade != null) {
        try {
          exploreFacade.dropPartition(datasetInstanceId, key);
        } catch (Exception e) {
          throw new DataSetException(String.format(
            "Unable to drop partition for key %s from explore table.", key.toString()), e);
        }
      }
    }
  }

  @Override
  public PartitionOutput getPartitionOutput(PartitionKey key) {
    return new BasicPartitionOutput(this, getOutputPath(partitioning, key), key);
  }

  @Override
  public PartitionDetail getPartition(PartitionKey key) {
    byte[] rowKey = generateRowKey(key, partitioning);
    Row row = partitionsTable.get(rowKey);
    if (row.isEmpty()) {
      return null;
    }

    byte[] pathBytes = row.get(RELATIVE_PATH);
    if (pathBytes == null) {
      return null;
    }

    return new BasicPartitionDetail(this, Bytes.toString(pathBytes), key, metadataFromRow(row));
  }

  @Override
  public Set<PartitionDetail> getPartitions(@Nullable PartitionFilter filter) {
    final Set<PartitionDetail> partitionDetails = Sets.newHashSet();
    getPartitions(filter, new PartitionConsumer() {
      @Override
      public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
        if (metadata == null) {
          metadata = new PartitionMetadata(Collections.<String, String>emptyMap(), 0L);
        }
        partitionDetails.add(new BasicPartitionDetail(PartitionedFileSetDataset.this, path, key, metadata
        ));
      }
    });
    return partitionDetails;
  }

  @VisibleForTesting
  Set<String> getPartitionPaths(@Nullable PartitionFilter filter) {
    // this avoids constructing the Partition object for every partition.
    final Set<String> paths = Sets.newHashSet();
    getPartitions(filter, new PartitionConsumer() {
      @Override
      public void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata) {
        paths.add(path);
      }
    }, false);
    return paths;
  }

  protected void getPartitions(@Nullable PartitionFilter filter, PartitionConsumer consumer) {
    // by default, parse the metadata from the rows
    getPartitions(filter, consumer, true);
  }

  // if decodeMetadata is false, null is passed as the PartitionMetadata to the PartitionConsumer,
  // for efficiency reasons, since the metadata is not always needed
  protected void getPartitions(@Nullable PartitionFilter filter, PartitionConsumer consumer, boolean decodeMetadata) {
    byte[] startKey = generateStartKey(filter);
    byte[] endKey = generateStopKey(filter);
    Scanner scanner = partitionsTable.scan(startKey, endKey);
    try {
      while (true) {
        Row row = scanner.next();
        if (row == null) {
          break;
        }
        PartitionKey key;
        try {
          key = parseRowKey(row.getRow(), partitioning);
        } catch (IllegalArgumentException e) {
          if (!ignoreInvalidRowsSilently) {
            LOG.debug(String.format("Failed to parse row key for partitioned file set '%s': %s",
                                    getName(), Bytes.toStringBinary(row.getRow())));
          }
          continue;
        }
        if (filter != null && !filter.match(key)) {
          continue;
        }
        byte[] pathBytes = row.get(RELATIVE_PATH);
        if (pathBytes != null) {
          consumer.consume(key, Bytes.toString(pathBytes), decodeMetadata ? metadataFromRow(row) : null);
        }
      }
    } finally {
      scanner.close();
    }
  }

  private PartitionMetadata metadataFromRow(Row row) {
    Map<String, String> metadata = Maps.newHashMap();
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      if (Bytes.startsWith(entry.getKey(), METADATA_PREFIX)) {
        String metadataKey = metadataKeyFromColumnKey(entry.getKey());
        metadata.put(metadataKey, Bytes.toString(entry.getValue()));
      }
    }

    byte[] creationTimeBytes = row.get(CREATION_TIME);
    return new PartitionMetadata(metadata, Bytes.toLong(creationTimeBytes));
  }

  private String metadataKeyFromColumnKey(byte[] columnKey) {
    return Bytes.toString(columnKey, METADATA_PREFIX.length, columnKey.length - METADATA_PREFIX.length);
  }

  private byte[] columnKeyFromMetadataKey(String metadataKey) {
    return Bytes.add(METADATA_PREFIX, Bytes.toBytes(metadataKey));
  }

  /**
   * Generate an output path for a given partition key.
   */
  // package visible for PartitionedFileSetDefinition
  static String getOutputPath(Partitioning partitioning, PartitionKey key) {
    StringBuilder builder = new StringBuilder();
    String sep = "";
    for (String fieldName : partitioning.getFields().keySet()) {
      builder.append(sep).append(key.getField(fieldName).toString());
      sep = "/";
    }
    return builder.toString();
  }

  /**
   * Interface use internally to build different types of results when scanning partitions.
   */
  protected interface PartitionConsumer {
    void consume(PartitionKey key, String path, @Nullable PartitionMetadata metadata);
  }

  @Override
  public void close() throws IOException {
    try {
      files.close();
    } finally {
      partitionsTable.close();
    }
  }

  @Override
  public String getInputFormatClassName() {
    return files.getInputFormatClassName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    PartitionFilter filter;
    try {
      filter = PartitionedFileSetArguments.getInputPartitionFilter(runtimeArguments, partitioning);
    } catch (Exception e) {
      throw new DataSetException("Partition filter must be correctly specified in arguments.");
    }
    Collection<String> inputPaths = getPartitionPaths(filter);
    List<Location> inputLocations = Lists.newArrayListWithExpectedSize(inputPaths.size());
    for (String path : inputPaths) {
      inputLocations.add(files.getLocation(path));
    }
    return files.getInputFormatConfiguration(inputLocations);
  }

  @Override
  public String getOutputFormatClassName() {
    return files.getOutputFormatClassName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    // we set the file set's output path in the definition's getDataset(), so there is no need to configure it again.
    // here we just want to validate that an output partition key was specified in the arguments.
    PartitionKey outputKey = PartitionedFileSetArguments.getOutputPartitionKey(runtimeArguments, getPartitioning());
    if (outputKey == null) {
      throw new DataSetException("Partition key must be given for the new output partition as a runtime argument.");
    }
    // copy the output partition key to the output arguments of the embedded file set
    // this will be needed by the output format to register the new partition.
    Map<String, String> config = files.getOutputFormatConfiguration();
    Map<String, String> outputArgs = Maps.newHashMap();
    outputArgs.putAll(config);
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
    return ImmutableMap.copyOf(outputArgs);
  }

  @Override
  public void onSuccess() throws DataSetException {
    String outputPath = FileSetArguments.getOutputPath(runtimeArguments);
    // if there if no output path, the batch job probably would have failed
    // we definitely can't do much here if we don't know the output path
    if (outputPath == null) {
      return;
    }
    // we know for sure there is an output partition key (checked in getOutputFormatConfig())
    PartitionKey outputKey = PartitionedFileSetArguments.getOutputPartitionKey(runtimeArguments, getPartitioning());
    // TODO: Add support to write to metadata from MapReduce - https://issues.cask.co/browse/CDAP-2784
    addPartition(outputKey, outputPath);
  }

  @Override
  public void onFailure() throws DataSetException {
    // nothing to do, as we have not written anything to the dataset yet
  }

  @Override
  public FileSet getEmbeddedFileSet() {
    return files;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  //------ private helpers below here --------------------------------------------------------------

  @VisibleForTesting
  static byte[] generateRowKey(PartitionKey key, Partitioning partitioning) {
    // validate partition key, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = partitionFields.size() - 1; // one \0 between each of the fields
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      Comparable fieldValue = key.getField(fieldName);
      if (fieldValue == null) {
        throw new IllegalArgumentException(
          String.format("Incomplete partition key: value for field '%s' is missing", fieldName));
      }
      if (!FieldTypes.validateType(fieldValue, fieldType)) {
        throw new IllegalArgumentException(
          String.format("Invalid partition key: value for %s field '%s' has incompatible type %s",
                        fieldType.name(), fieldName, fieldValue.getClass().getName()));
      }
      byte[] bytes = FieldTypes.toBytes(fieldValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
    }
    byte[] rowKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, rowKey, offset, bytes.length);
      offset += bytes.length + 1; // this leaves a \0 byte after the value
    }
    return rowKey;
  }

  private byte[] generateStartKey(PartitionFilter filter) {
    if (null == filter) {
      return null;
    }
    // validate partition filter, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = 0;
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(fieldName);
      if (condition == null) {
        break; // this field is not present; we can't include any more fields in the start key
      }
      Comparable lowerValue = condition.getLower();
      if (lowerValue == null) {
        break; // this field has no lower bound; we can't include any more fields in the start key
      }
      if (!FieldTypes.validateType(lowerValue, fieldType)) {
        throw new IllegalArgumentException(
          String.format("Invalid partition filter: lower bound for %s field '%s' has incompatible type %s",
                        fieldType.name(), fieldName, lowerValue.getClass().getName()));
      }
      byte[] bytes = FieldTypes.toBytes(lowerValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
    }
    if (values.isEmpty()) {
      return null;
    }
    totalSize += values.size() - 1; // one \0 between each of the fields
    byte[] startKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, startKey, offset, bytes.length);
      offset += bytes.length + 1; // this leaves a \0 byte after the value
    }
    return startKey;
  }

  private byte[] generateStopKey(PartitionFilter filter) {
    if (null == filter) {
      return null;
    }
    // validate partition filter, convert values, and compute size of output
    Map<String, FieldType> partitionFields = partitioning.getFields();
    int totalSize = 0;
    boolean allSingleValue = true;
    ArrayList<byte[]> values = Lists.newArrayListWithCapacity(partitionFields.size());
    for (Map.Entry<String, FieldType> entry : partitionFields.entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(fieldName);
      if (condition == null) {
        break; // this field is not present; we can't include any more fields in the stop key
      }
      Comparable upperValue = condition.getUpper();
      if (upperValue == null) {
        break; // this field is not present; we can't include any more fields in the stop key
      }
      if (!FieldTypes.validateType(upperValue, fieldType)) {
        throw new IllegalArgumentException(
          String.format("Invalid partition filter: upper bound for %s field '%s' has incompatible type %s",
                        fieldType.name(), fieldName, upperValue.getClass().getName()));
      }
      byte[] bytes = FieldTypes.toBytes(upperValue, fieldType);
      totalSize += bytes.length;
      values.add(bytes);
      if (!condition.isSingleValue()) {
        allSingleValue = false;
        break; // upper bound for this field, following fields don't matter
      }
    }
    if (values.isEmpty()) {
      return null;
    }
    totalSize += values.size() - 1; // one \0 between each of the fields
    if (allSingleValue) {
      totalSize++; // in this case the start and stop key are equal, we append one \1 to ensure the scan is not empty
    }
    byte[] stopKey = new byte[totalSize];
    int offset = 0;
    for (byte[] bytes : values) {
      System.arraycopy(bytes, 0, stopKey, offset, bytes.length);
      offset += bytes.length + 1; // this leaves a \0 byte after the value
      if (allSingleValue && offset == stopKey.length) {
        stopKey[offset - 1] = 1; // see above - we \1 instead of \0 at the end, to make sure scan is not empty
      }
    }
    return stopKey;
  }

  @VisibleForTesting
  static PartitionKey parseRowKey(byte[] rowKey, Partitioning partitioning) {
    PartitionKey.Builder builder = PartitionKey.builder();
    int offset = 0;
    boolean first = true;
    for (Map.Entry<String, FieldType> entry : partitioning.getFields().entrySet()) {
      String fieldName = entry.getKey();
      FieldType fieldType = entry.getValue();
      if (!first) {
        if (offset >= rowKey.length) {
          throw new IllegalArgumentException(
            String.format("Invalid row key: Expecting field '%s' at offset %d " +
                            "but the end of the row key is reached.", fieldName, offset));
        }
        if (rowKey[offset] != 0) {
          throw new IllegalArgumentException(
            String.format("Invalid row key: Expecting field separator \\0 before field '%s' at offset %d " +
                            "but found byte value %x.", fieldName, offset, rowKey[offset]));
        }
        offset++;
      }
      first = false;
      int size = FieldTypes.determineLengthInBytes(rowKey, offset, fieldType);
      if (size + offset > rowKey.length) {
        throw new IllegalArgumentException(
          String.format("Invalid row key: Expecting field '%s' of type %s, " +
                          "requiring %d bytes at offset %d, but only %d bytes remain.",
                        fieldName, fieldType.name(), size, offset, rowKey.length - offset));
      }
      Comparable fieldValue = FieldTypes.fromBytes(rowKey, offset, size, fieldType);
      offset += size;
      builder.addField(fieldName, fieldValue);
    }
    if (offset != rowKey.length) {
      throw new IllegalArgumentException(
        String.format("Invalid row key: Read all fields at offset %d but %d extra bytes remain.",
                      offset, rowKey.length - offset));
    }
    return builder.build();
  }

  /**
   * Simple Implementation of PartitionOutput.
   */
  protected static class BasicPartitionOutput extends BasicPartition implements PartitionOutput {
    private Map<String, String> metadata;

    protected BasicPartitionOutput(PartitionedFileSetDataset partitionedFileSetDataset, String relativePath,
                                   PartitionKey key) {
      super(partitionedFileSetDataset, relativePath, key);
      this.metadata = Maps.newHashMap();
    }

    @Override
    public void addPartition() {
      partitionedFileSetDataset.addPartition(key, getRelativePath(), metadata);
    }

    @Override
    public void setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
    }
  }
}
