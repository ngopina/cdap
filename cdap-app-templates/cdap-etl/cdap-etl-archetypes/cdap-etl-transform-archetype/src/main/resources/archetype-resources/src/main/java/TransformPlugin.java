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


package $package;

import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.Transform;

/**
 * ETL Transform.
 */
public class TransformPlugin<T> extends Transform<T, T> {

  @Override
  public void transform(T input, Emitter<T> emitter) throws Exception {
    // Perform transformation
    //emitter.emit(value);
  }
}

