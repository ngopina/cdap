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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginVersion;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;
import javax.annotation.Nullable;

/**
 * This class manage plugin information that are available for application templates.
 */
public class PluginRepository {

  private static final Logger LOG = LoggerFactory.getLogger(PluginRepository.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(PluginClass.class, new PluginClassDeserializer())
    .create();

  // Object type for data in the config json file.
  private static final Type CONFIG_OBJECT_TYPE = new TypeToken<List<PluginClass>>() { }.getType();

  // Transform File into PluginInfo, assuming the plugin file name is in form [name][separator][version].jar
  private static final Function<File, PluginFile> FILE_TO_PLUGIN_FILE = new Function<File, PluginFile>() {
    @Override
    public PluginFile apply(File file) {
      String plugin = file.getName().substring(0, file.getName().length() - ".jar".length());
      PluginVersion version = new PluginVersion(plugin, true);
      String rawVersion = version.getVersion();

      String pluginName = rawVersion == null ? plugin : plugin.substring(0, plugin.length() - rawVersion.length() - 1);
      return new PluginFile(file, new PluginInfo(file.getName(), pluginName, version));
    }
  };

  private final CConfiguration cConf;
  private final File templateDir;
  private final File tmpDir;
  private final AtomicReference<Map<String, Multimap<PluginInfo, PluginClass>>> plugins;

  @Inject
  PluginRepository(CConfiguration cConf) {
    this.cConf = cConf;
    this.templateDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    this.plugins = new AtomicReference<Map<String, Multimap<PluginInfo, PluginClass>>>(
      new HashMap<String, Multimap<PluginInfo, PluginClass>>());
    this.tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
  }

  /**
   * Returns a {@link Multimap} of all plugins available for the given application template.
   */
  public Multimap<PluginInfo, PluginClass> getPlugins(String template) {
    Multimap<PluginInfo, PluginClass> result = plugins.get().get(template);
    return result == null ? ImmutableMultimap.<PluginInfo, PluginClass>of() : result;
  }

  /**
   * Inspects plugins for all the templates.
   *
   * @param templates map from template name to template jar file
   */
  void inspectPlugins(Map<String, File> templates) throws IOException {
    Map<String, Multimap<PluginInfo, PluginClass>> result = Maps.newHashMap();
    for (Map.Entry<String, File> entry : templates.entrySet()) {
      result.put(entry.getKey(), inspectPlugins(entry.getKey(), entry.getValue()));
    }

    plugins.set(result);
  }

  /**
   * Inspects and builds plugin information for the given application template.
   *
   * @param template name of the template
   * @param templateJar application jar for the application template
   */
  private Multimap<PluginInfo, PluginClass> inspectPlugins(String template, File templateJar) throws IOException {
    // We want the plugins sorted by the PluginInfo, which in turn is sorted by name and version.
    Multimap<PluginInfo, PluginClass> templatePlugins = Multimaps.newMultimap(
      Maps.<PluginInfo, Collection<PluginClass>>newTreeMap(), new Supplier<Collection<PluginClass>>() {
        @Override
        public Collection<PluginClass> get() {
          return Lists.newArrayList();
        }
      });

    File pluginDir = new File(templateDir, template);
    List<File> pluginJars = DirUtils.listFiles(pluginDir, "jar");
    if (pluginJars.isEmpty()) {
      return templatePlugins;
    }

    Iterable<PluginFile> pluginFiles = Iterables.transform(pluginJars, FILE_TO_PLUGIN_FILE);
    CloseableClassLoader templateClassLoader = createTemplateClassLoader(templateJar);
    try {
      PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf, template, templateClassLoader);
      try {
        for (PluginFile pluginFile : pluginFiles) {
          if (!configureByFile(pluginFile, templatePlugins)) {
            configureByInspection(pluginFile, pluginInstantiator, templatePlugins);
          }
        }
      } finally {
        pluginInstantiator.close();
      }
    } finally {
      templateClassLoader.close();
    }

    return templatePlugins;
  }

  /**
   * Gathers plugin class information by parsing an external configuration file.
   *
   * @return {@code true} if there is an external configuration file, {@code false} otherwise.
   */
  private boolean configureByFile(PluginFile pluginFile,
                                  Multimap<PluginInfo, PluginClass> templatePlugins) throws IOException {
    String pluginFileName = pluginFile.getFile().getName();
    String configFileName = pluginFileName.substring(0, pluginFileName.length() - ".jar".length()) + ".json";

    File configFile = new File(pluginFile.getFile().getParentFile(), configFileName);
    if (!configFile.isFile()) {
      return false;
    }

    // The config file is a json array of PluginClass object (except the PluginClass.configFieldName)
    Reader reader = Files.newReader(configFile, Charsets.UTF_8);
    try {
      List<PluginClass> pluginClasses = GSON.fromJson(reader, CONFIG_OBJECT_TYPE);
      templatePlugins.putAll(pluginFile.getPluginInfo(), pluginClasses);
    } finally {
      Closeables.closeQuietly(reader);
    }

    return true;
  }

  /**
   * Inspects the plugin file and extracts plugin classes information.
   */
  private void configureByInspection(PluginFile pluginFile, PluginInstantiator pluginInstantiator,
                                     Multimap<PluginInfo, PluginClass> templatePlugins) throws IOException {

    // See if there are export packages. Plugins should be in those packages
    Set<String> exportPackages = getExportPackages(pluginFile.getFile());
    if (exportPackages.isEmpty()) {
      return;
    }

    // Load the plugin class and inspect the config field.
    ClassLoader pluginClassLoader = pluginInstantiator.getPluginClassLoader(pluginFile.getPluginInfo());
    for (Class<?> pluginClass : getPluginClasses(exportPackages, pluginClassLoader)) {
      Plugin pluginAnnotation = pluginClass.getAnnotation(Plugin.class);
      if (pluginAnnotation == null) {
        continue;
      }
      Map<String, PluginPropertyField> pluginProperties = Maps.newHashMap();
      try {
        String configField = getProperties(TypeToken.of(pluginClass), pluginProperties);
        templatePlugins.put(pluginFile.getPluginInfo(), new PluginClass(pluginAnnotation.type(),
                                                                        getPluginName(pluginClass),
                                                                        getPluginDescription(pluginClass),
                                                                        pluginClass.getName(),
                                                                        configField, pluginProperties));
      } catch (UnsupportedTypeException e) {
        LOG.warn("Plugin configuration type not supported. Plugin ignored. {}", pluginClass, e);
      }
    }
  }

  /**
   * Returns the set of package names that are declared in "Export-Package" in the jar file Manifest.
   */
  private Set<String> getExportPackages(File file) throws IOException {
    JarFile jarFile = new JarFile(file);
    try {
      return ManifestFields.getExportPackages(jarFile.getManifest());
    } finally {
      jarFile.close();
    }
  }

  /**
   * Returns an {@link Iterable} of class name that are under the given list of package names that are loadable
   * through the plugin ClassLoader.
   */
  private Iterable<Class<?>> getPluginClasses(final Iterable<String> packages, final ClassLoader pluginClassLoader) {
    return new Iterable<Class<?>>() {
      @Override
      public Iterator<Class<?>> iterator() {
        final Iterator<String> packageIterator = packages.iterator();

        return new AbstractIterator<Class<?>>() {
          Iterator<String> classIterator = ImmutableList.<String>of().iterator();
          String currentPackage;

          @Override
          protected Class<?> computeNext() {
            while (!classIterator.hasNext()) {
              if (!packageIterator.hasNext()) {
                return endOfData();
              }
              currentPackage = packageIterator.next();

              URL packageResource = pluginClassLoader.getResource(currentPackage.replace('.', File.separatorChar));
              if (packageResource == null) {
                // Cannot happen since we know the class loader expand the jar into directory, the class loader
                // should have the package URL pointing to the package directory.
                continue;
              }

              try {
                classIterator = DirUtils.list(new File(packageResource.toURI()), "class").iterator();
              } catch (URISyntaxException e) {
                // Cannot happen
                throw Throwables.propagate(e);
              }
            }

            try {
              return pluginClassLoader.loadClass(getClassName(currentPackage, classIterator.next()));
            } catch (ClassNotFoundException e) {
              // Cannot happen, since the class name is from the list of the class files under the classloader.
              throw Throwables.propagate(e);
            }
          }
        };
      }
    };
  }

  /**
   * Creates a ClassLoader for the given template application.
   *
   * @param templateJar the template jar file.
   * @return a {@link CloseableClassLoader} for the template application.
   * @throws IOException if failed to expand the jar
   */
  private CloseableClassLoader createTemplateClassLoader(File templateJar) throws IOException {
    final File unpackDir = DirUtils.createTempDir(tmpDir);
    BundleJarUtil.unpackProgramJar(Files.newInputStreamSupplier(templateJar), unpackDir);
    ProgramClassLoader programClassLoader = ProgramClassLoader.create(unpackDir, getClass().getClassLoader());
    return new CloseableClassLoader(programClassLoader, new Closeable() {
      @Override
      public void close() throws IOException {
        DirUtils.deleteDirectoryContents(unpackDir);
      }
    });
  }

  /**
   * Extracts and returns name of the plugin.
   */
  private String getPluginName(Class<?> cls) {
    Name annotation = cls.getAnnotation(Name.class);
    return annotation == null || annotation.value().isEmpty() ? cls.getName() : annotation.value();
  }

  /**
   * Returns description for the plugin.
   */
  private String getPluginDescription(Class<?> cls) {
    Description annotation = cls.getAnnotation(Description.class);
    return annotation == null ? "" : annotation.value();
  }

  /**
   * Constructs the fully qualified class name based on the package name and the class file name.
   */
  private String getClassName(String packageName, String classFileName) {
    return packageName + "." + classFileName.substring(0, classFileName.length() - ".class".length());
  }

  /**
   * Gets all config properties for the given plugin.
   *
   * @return the name of the config field in the plugin class or {@code null} if the plugin doesn't have a config field
   */
  @Nullable
  private String getProperties(TypeToken<?> pluginType,
                               Map<String, PluginPropertyField> result) throws UnsupportedTypeException {
    // Get the config field
    for (TypeToken<?> type : pluginType.getTypes().classes()) {
      for (Field field : type.getRawType().getDeclaredFields()) {
        TypeToken<?> fieldType = TypeToken.of(field.getGenericType());
        if (PluginConfig.class.isAssignableFrom(fieldType.getRawType())) {
          // Pick up all config properties
          inspectConfigField(fieldType, result);
          return field.getName();
        }
      }
    }
    return null;
  }

  /**
   * Inspects the plugin config class and build up a map for {@link PluginPropertyField}.
   *
   * @param configType type of the config class
   * @param result map for storing the result
   * @throws UnsupportedTypeException if a field type in the config class is not supported
   */
  private void inspectConfigField(TypeToken<?> configType,
                                  Map<String, PluginPropertyField> result) throws UnsupportedTypeException {
    for (TypeToken<?> type : configType.getTypes().classes()) {
      if (PluginConfig.class.equals(type.getRawType())) {
        break;
      }

      for (Field field : type.getRawType().getDeclaredFields()) {
        PluginPropertyField property = createPluginProperty(field, type);
        if (result.containsKey(property.getName())) {
          throw new IllegalArgumentException("Plugin config with name " + property.getName()
                                               + " already defined in " + configType.getRawType());
        }
        result.put(property.getName(), property);
      }
    }
  }

  /**
   * Creates a {@link PluginPropertyField} based on the given field.
   */
  private PluginPropertyField createPluginProperty(Field field,
                                                   TypeToken<?> resolvingType) throws UnsupportedTypeException {
    TypeToken<?> fieldType = resolvingType.resolveType(field.getGenericType());
    Class<?> rawType = fieldType.getRawType();

    Name nameAnnotation = field.getAnnotation(Name.class);
    Description descAnnotation = field.getAnnotation(Description.class);
    String name = nameAnnotation == null ? field.getName() : nameAnnotation.value();
    String description = descAnnotation == null ? "" : descAnnotation.value();

    if (rawType.isPrimitive()) {
      return new PluginPropertyField(name, description, rawType.getName(), true);
    }

    rawType = Primitives.unwrap(rawType);
    if (!rawType.isPrimitive() && !String.class.equals(rawType)) {
      throw new UnsupportedTypeException("Only primitive and String types are supported");
    }

    boolean required = true;
    for (Annotation annotation : field.getAnnotations()) {
      if (annotation.annotationType().getName().endsWith(".Nullable")) {
        required = false;
        break;
      }
    }

    return new PluginPropertyField(name, description, rawType.getSimpleName().toLowerCase(), required);
  }

  /**
   * A {@link ClassLoader} that implements {@link Closeable} for resource cleanup. All classloading is done
   * by the delegate {@link ClassLoader}.
   */
  private static final class CloseableClassLoader extends ClassLoader implements Closeable {

    private final Closeable closeable;

    public CloseableClassLoader(ClassLoader delegate, Closeable closeable) {
      super(delegate);
      this.closeable = closeable;
    }

    @Override
    public void close() throws IOException {
      closeable.close();
    }
  }

  /**
   * A Gson deserialization for creating {@link PluginClass} object from external plugin config file.
   */
  private static final class PluginClassDeserializer implements JsonDeserializer<PluginClass> {

    // Type for the PluginClass.properties map.
    private static final Type PROPERTIES_TYPE = new TypeToken<Map<String, PluginPropertyField>>() { }.getType();

    @Override
    public PluginClass deserialize(JsonElement json, Type typeOfT,
                                   JsonDeserializationContext context) throws JsonParseException {
      if (!json.isJsonObject()) {
        throw new JsonParseException("Expects json object");
      }

      JsonObject jsonObj = json.getAsJsonObject();

      String type = jsonObj.has("type") ? jsonObj.get("type").getAsString() : Plugin.DEFAULT_TYPE;
      String name = getRequired(jsonObj, "name").getAsString();
      String description = getRequired(jsonObj, "description").getAsString();
      String className = getRequired(jsonObj, "className").getAsString();
      Map<String, PluginPropertyField> properties = context.deserialize(getRequired(jsonObj, "properties"),
                                                                        PROPERTIES_TYPE);

      return new PluginClass(type, name, description, className, null, properties);
    }

    private JsonElement getRequired(JsonObject jsonObj, String name) {
      if (!jsonObj.has(name)) {
        throw new JsonParseException("Property '" + name + "' is missing");
      }
      return jsonObj.get(name);
    }
  }
}