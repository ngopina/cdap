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

package co.cask.cdap.internal.api.namespace;

import co.cask.cdap.common.exception.NamespaceAlreadyExistsException;
import co.cask.cdap.common.exception.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.exception.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;

import java.util.List;

/**
 * Admin class for managing a namespace's lifecycle
 */
public interface NamespaceAdmin {

  /**
   * Lists all namespaces
   *
   * @return a list of {@link NamespaceMeta} for all namespaces
   */
  List<NamespaceMeta> listNamespaces() throws Exception;

  /**
   * Gets details of a namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the requested namespace
   * @return the {@link NamespaceMeta} of the requested namespace
   * @throws NamespaceNotFoundException if the requested namespace is not found
   */
  NamespaceMeta getNamespace(Id.Namespace namespaceId) throws Exception;

  /**
   * Checks if the specified namespace exists
   *
   * @param namespaceId the {@link Id.Namespace} to check for existence
   * @return true, if the specifed namespace exists, false otherwise
   */
  boolean hasNamespace(Id.Namespace namespaceId) throws Exception;

  /**
   * Creates a new namespace
   *
   * @param metadata the {@link NamespaceMeta} for the new namespace to be created
   * @throws NamespaceAlreadyExistsException if the specified namespace already exists
   * @throws NamespaceCannotBeCreatedException if the creation operation was unsuccessful
   */
  void createNamespace(NamespaceMeta metadata) throws Exception;

  /**
   * Deletes the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NamespaceNotFoundException if the specified namespace does not exist
   * @throws NamespaceCannotBeDeletedException if the deletion operation was unsuccessful
   */
  void deleteNamespace(Id.Namespace namespaceId) throws Exception;

  /**
   * Deletes all datasets in the specified namespace
   *
   * @param namespaceId the {@link Id.Namespace} of the specified namespace
   * @throws NotFoundException if the specified namespace does not exist
   * @throws NamespaceCannotBeDeletedException if the deletion operation was unsuccessful
   */
  void deleteDatasets(Id.Namespace namespaceId) throws Exception;

  /**
   * Update namespace properties for a given namespace.
   *
   * @param namespaceId  the {@link Id.Namespace} of the namespace to be updated
   * @param namespaceMeta namespacemeta to update
   * @throws NotFoundException if the specified namespace is not found
   */
  void updateProperties(Id.Namespace namespaceId, NamespaceMeta namespaceMeta) throws Exception;
}
