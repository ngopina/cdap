/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.hive.datasets;

import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.hive.context.ConfigurationUtil;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.hive.context.NullJobConfException;
import co.cask.cdap.hive.context.TxnCodec;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helps in instantiating a dataset.
 */
public class DatasetAccessor {
  private static final SchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();

  // TODO: this will go away when dataset manager does not return datasets having classloader conflict - CDAP-10
  // Map of queryHandle -> datasetName -> classloader
  private static final LoadingCache<QueryHandle, Map<Id.DatasetInstance, ClassLoader>> DATASET_CLASSLOADER_MAP =
    CacheBuilder.newBuilder().build(
      new CacheLoader<QueryHandle, Map<Id.DatasetInstance, ClassLoader>>() {
        @Override
        public Map<Id.DatasetInstance, ClassLoader> load(QueryHandle key) throws Exception {
          return Maps.newConcurrentMap();
        }
      }
    );

  /**
   * Returns a RecordScannable dataset. The returned object will have to be closed by the caller.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configuration.
   * @return RecordScannable which name is contained in the {@code conf}.
   * @throws IOException in case the conf does not contain a valid RecordScannable.
   */
  public static RecordScannable getRecordScannable(Configuration conf) throws IOException {
    // todo: id should be passed in. along with a lot of other cleanup for this class
    Id.DatasetInstance datasetInstanceId = getDatasetInstanceId(conf);
    Dataset dataset = instantiate(conf, datasetInstanceId);

    if (!(dataset instanceof RecordScannable)) {
      throw new IOException(
        String.format("Dataset %s does not implement RecordScannable, and hence cannot be queried in Hive.",
                      getDatasetInstanceId(conf)));
    }

    RecordScannable recordScannable = (RecordScannable) dataset;

    if (recordScannable instanceof TransactionAware) {
      startTransaction(conf, (TransactionAware) recordScannable);
    }

    return recordScannable;
  }

  /**
   * Returns a RecordWritable dataset. The returned object will have to be closed by the caller.
   *
   * @param conf Configuration that contains RecordWritable name to load, CDAP and HBase configurations.
   * @return RecordWritable which name is contained in the {@code conf}.
   * @throws IOException in case the conf does not contain a valid RecordWritable.
   */
  public static RecordWritable getRecordWritable(Configuration conf) throws IOException {
    RecordWritable recordWritable = instantiateWritable(conf, null);

    if (recordWritable instanceof TransactionAware) {
      startTransaction(conf, (TransactionAware) recordWritable);
    }
    return recordWritable;
  }

  /**
   * Check that the conf contains information about a valid RecordWritable object.
   * @param conf configuration containing RecordWritable name, CDAP and HBase configurations.
   * @throws IOException in case the conf does not contain a valid RecordWritable.
   */
  public static void checkRecordWritable(Configuration conf) throws IOException {
    RecordWritable recordWritable = instantiateWritable(conf, null);
    if (recordWritable != null) {
      recordWritable.close();
    }
  }

  /**
   * Returns record type of the RecordScannable.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configurations.
   * @return Record type of RecordScannable dataset.
   * @throws IOException in case the conf does not contain a valid RecordScannable.
   */
  private static Type getRecordType(Configuration conf, Id.DatasetInstance id) throws IOException {
    try (Dataset dataset = instantiate(conf, id)) {
      if (dataset instanceof RecordWritable) {
        return ((RecordWritable) dataset).getRecordType();
      } else if (dataset instanceof RecordScannable) {
        return ((RecordScannable) dataset).getRecordType();
      }
      throw new IOException(
        String.format("Dataset %s does not implement neither RecordScannable nor RecordWritable.",
                      getDatasetInstanceId(conf)));
    }
  }

  /**
   * Returns the schema of the dataset. Uses the schema property if it is set, otherwise derives the schema from
   * the record type.
   *
   * @param conf Configuration that contains RecordScannable name to load, CDAP and HBase configurations.
   * @return Schema of the dataset
   * @throws IOException in case the dataset does not contain a valid schema
   * @throws UnsupportedTypeException in case the record type generates an unsupported schema
   */
  public static Schema getRecordSchema(Configuration conf, Id.DatasetInstance id)
    throws UnsupportedTypeException, IOException {
    // get the schema from the dataset properties if it's there
    DatasetSpecification spec = getDatasetSpec(conf, id);
    if (spec != null) {
      String schemaStr = spec.getProperty("schema");
      if (schemaStr != null) {
        return Schema.parseJson(schemaStr);
      }
    }

    // otherwise try to derive the schema from the type
    Type type = getRecordType(conf, id);
    return schemaGenerator.generate(type);
  }

  /**
   * Release Dataset resources associated with a query.
   * @param queryHandle query handle.
   */
  public static void closeQuery(QueryHandle queryHandle) {
    DATASET_CLASSLOADER_MAP.invalidate(queryHandle);
  }

  /**
   * Release Dataset resources associated with all queries till now.
   */
  public static void closeAllQueries() {
    DATASET_CLASSLOADER_MAP.invalidateAll();
  }

  private static void startTransaction(Configuration conf, TransactionAware txAware) throws IOException {
    Transaction tx = ConfigurationUtil.get(conf, Constants.Explore.TX_QUERY_KEY, TxnCodec.INSTANCE);
    txAware.startTx(tx);
  }

  private static RecordWritable instantiateWritable(@Nullable Configuration conf, Id.DatasetInstance datasetInstanceId)
    throws IOException {
    Dataset dataset = instantiate(conf, datasetInstanceId);

    if (!(dataset instanceof RecordWritable)) {
      dataset.close();
      throw new IOException(
        String.format("Dataset %s does not implement RecordWritable, and hence cannot be written to in Hive.",
                      datasetInstanceId != null ? datasetInstanceId : getDatasetInstanceId(conf)));
    }
    return (RecordWritable) dataset;
  }

  private static DatasetSpecification getDatasetSpec(Configuration conf, Id.DatasetInstance id) throws IOException {
    ContextManager.Context context = ContextManager.getContext(conf);
    if (context == null) {
      throw new NullJobConfException();
    }

    try {
      DatasetFramework framework = context.getDatasetFramework();
      return framework.getDatasetSpec(id);
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    } finally {
      context.close();
    }
  }

  private static Dataset instantiate(@Nullable Configuration conf, Id.DatasetInstance dsInstanceId)
    throws IOException {
    ContextManager.Context context = ContextManager.getContext(conf);
    if (context == null) {
      throw new NullJobConfException();
    }
    
    Id.DatasetInstance datasetInstanceId = dsInstanceId != null ? dsInstanceId : getDatasetInstanceId(conf);

    if (datasetInstanceId == null) {
      throw new IOException("Dataset name property could not be found.");
    }
    
    try {
      DatasetFramework framework = context.getDatasetFramework();

      if (conf == null) {
        return framework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, null);
      }

      String queryId = conf.get(Constants.Explore.QUERY_ID);
      if (queryId == null) {
        throw new IOException("QueryId property could not be found");
      }
      QueryHandle queryHandle = QueryHandle.fromId(queryId);
      ClassLoader classLoader = DATASET_CLASSLOADER_MAP.getUnchecked(queryHandle).get(datasetInstanceId);
      Dataset dataset;
      if (classLoader == null) {
        classLoader = conf.getClassLoader();
        dataset = firstLoad(framework, queryHandle, datasetInstanceId, classLoader);
      } else {
        dataset = framework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, classLoader);
      }
      return dataset;
    } catch (DatasetManagementException e) {
      throw new IOException(e);
    } finally {
      context.close();
    }
  }
  
  private static Id.DatasetInstance getDatasetInstanceId(@Nullable Configuration conf) {
    return conf == null ? null : Id.DatasetInstance.from(conf.get(Constants.Explore.DATASET_NAMESPACE),
                                                         conf.get(Constants.Explore.DATASET_NAME));
  }

  private static synchronized Dataset firstLoad(DatasetFramework framework, QueryHandle queryHandle, 
                                                Id.DatasetInstance datasetInstanceId, ClassLoader classLoader)
    
    throws DatasetManagementException, IOException {
    ClassLoader datasetClassLoader = DATASET_CLASSLOADER_MAP.getUnchecked(queryHandle).get(datasetInstanceId);
    if (datasetClassLoader != null) {
      // Some other call in parallel may have already loaded it, so use the same classlaoder
      return framework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, datasetClassLoader);
    }

    // No classloader for dataset exists, load the dataset and save the classloader.
    Dataset dataset = framework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, classLoader);
    if (dataset != null) {
      DATASET_CLASSLOADER_MAP.getUnchecked(queryHandle).put(datasetInstanceId, dataset.getClass().getClassLoader());
    }
    return dataset;
  }
}
