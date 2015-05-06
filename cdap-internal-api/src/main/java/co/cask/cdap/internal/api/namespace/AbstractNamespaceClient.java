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

import co.cask.cdap.common.exception.BadRequestException;
import co.cask.cdap.common.exception.NamespaceAlreadyExistsException;
import co.cask.cdap.common.exception.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.exception.NamespaceNotFoundException;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Common implementation of methods to interact with namespace service.
 */
public abstract class AbstractNamespaceClient implements NamespaceAdmin {
  private static final Gson GSON = new Gson();

  protected abstract HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException;
  protected abstract URL resolve(String resource) throws IOException;

  @Override
  public List<NamespaceMeta> listNamespaces() throws Exception {
    HttpRequest request = HttpRequest.get(resolve("namespaces")).build();
    HttpResponse response = execute(request);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, new TypeToken<List<NamespaceMeta>>() { })
        .getResponseObject();
    }
    throw new IOException(String.format("Cannot list namespaces. Reason: %s", response));
  }

  @Override
  public NamespaceMeta getNamespace(Id.Namespace namespaceId) throws Exception {
    HttpResponse response =
      execute(HttpRequest.get(resolve(String.format("namespaces/%s", namespaceId.getId()))).build());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return ObjectResponse.fromJsonBody(response, NamespaceMeta.class).getResponseObject();
    }
    throw new IOException("Cannot get namespace. Reason: " + response);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws Exception {
    URL url = resolve(String.format("unrecoverable/namespaces/%s", namespaceId.getId()));
    HttpResponse response = execute(HttpRequest.delete(url).build());

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    } else if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new NamespaceCannotBeDeletedException(namespaceId, response.getResponseBodyAsString());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    throw new IOException("Cannot delete namespace. Reason: " + response);
  }

  @Override
  public void createNamespace(NamespaceMeta metadata) throws Exception {
    Id.Namespace namespace = Id.Namespace.from(metadata.getName());
    URL url = resolve(String.format("namespaces/%s", namespace.getId()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(metadata)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      if (responseBody.equals(String.format("Namespace '%s' already exists.", metadata.getName()))) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException(String.format("Cannot create namespace %s. Reason: %s", metadata.getName(), response));
  }

  @Override
  public boolean hasNamespace(Id.Namespace namespaceId) throws Exception {
    return getNamespace(namespaceId) != null;
  }

  @Override
  public void updateProperties(Id.Namespace namespaceId, NamespaceMeta metadata) throws Exception {
    URL url = resolve(String.format("namespaces/%s/properties", namespaceId.getId()));
    HttpResponse response = execute(HttpRequest.put(url).withBody(GSON.toJson(metadata)).build());
    String responseBody = response.getResponseBodyAsString();
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException("Bad request: " + responseBody);
    }
    throw new IOException("Cannot get create namespace. Reason: " + "getDetails(response)");
  }

  @Override
  public void deleteDatasets(Id.Namespace namespaceId) throws Exception {
    URL url = resolve(String.format("unrecoverable/namespaces/%s/datasets", namespaceId.getId()));
    HttpResponse response = execute(HttpRequest.delete(url).build());

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NamespaceNotFoundException(namespaceId);
    } else if (HttpURLConnection.HTTP_FORBIDDEN == response.getResponseCode()) {
      throw new NamespaceCannotBeDeletedException(namespaceId, response.getResponseBodyAsString());
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    }
    throw new IOException("Cannot delete namespace. Reason: " + response.getResponseBodyAsString(Charsets.UTF_8));
  }

  public void deleteAll() throws Exception {
    for (NamespaceMeta meta : listNamespaces()) {
      deleteNamespace(Id.Namespace.from(meta.getName()));
    }
  }
}
