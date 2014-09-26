/**
 * Copyright 2012 plista GmbH  (http://www.plista.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 */

package org.plista.kornakapi.core.config;

import com.google.common.collect.Lists;
import com.thoughtworks.xstream.XStream;

import java.util.List;



/** basic configuration object for the weblayer */
public class Configuration {

  /** directory to store the models in */
  private String modelDirectory;
  /** number of cores to use for training */
  private int numProcessorsForTraining;
  
  /** if true, preference values can only be overwritten by bigger values*/
  private boolean maxPersistence;
  
  /** linear rating decay*/
  private float ratingDecay;

  private StorageConfiguration storageConfiguration;

  private List<ItembasedRecommenderConfig> itembasedRecommenders = Lists.newArrayList();
  private List<FactorizationbasedRecommenderConfig> factorizationbasedRecommenders = Lists.newArrayList();
  private List<StreamingKMeansClustererConfig> streamingKMeansClusterers = Lists.newArrayList();
  private FactorizationbasedOptimizerConfig factorizationbasedOptimizer = null;
  private LDARecommenderConfig ldaRecommender= null;

  public static Configuration fromXML(String xml) {
    XStream serializer = new XStream();
    serializer.alias("configuration", Configuration.class);
    serializer.alias("itembasedRecommender", ItembasedRecommenderConfig.class);
    serializer.alias("factorizationbasedRecommender", FactorizationbasedRecommenderConfig.class);
    serializer.alias("streamingKMeansClusterer", StreamingKMeansClustererConfig.class);
    serializer.alias("factorizationbasedOptimizer", FactorizationbasedOptimizerConfig.class);
    serializer.alias("ldaRecommender", LDARecommenderConfig.class);

    return (Configuration) serializer.fromXML(xml);
  }

  public String getModelDirectory() {
    return modelDirectory;
  }
  
  public List<ItembasedRecommenderConfig> getItembasedRecommenders() {
    return itembasedRecommenders;
  }

  public void addItembasedRecommender(ItembasedRecommenderConfig itembasedRecommender) {
    itembasedRecommenders.add(itembasedRecommender);
  }

  public List<FactorizationbasedRecommenderConfig> getFactorizationbasedRecommenders() {
    return factorizationbasedRecommenders;
  }
  
  public List<StreamingKMeansClustererConfig> getStreamingKMeansClusterer() {
	    return streamingKMeansClusterers;
	  }
  
  public void addStreamingKMeansClusterer(StreamingKMeansClustererConfig StreamingKMeansClusterer){
	  streamingKMeansClusterers.add(StreamingKMeansClusterer);
  }

  public void addFactorizationbasedRecommender(FactorizationbasedRecommenderConfig factorizationbasedRecommender) {
    factorizationbasedRecommenders.add(factorizationbasedRecommender);
  }
  
  public void setFactorizationbasedOptimizer(FactorizationbasedOptimizerConfig factorizationbasedOptimizerConfig){
	  this.factorizationbasedOptimizer = factorizationbasedOptimizerConfig;
  }
  
  public FactorizationbasedOptimizerConfig getFactorizationbasedOptimizer(){
	  return factorizationbasedOptimizer;
  }
  
  public void setLDARecommenderConfig(LDARecommenderConfig factorizationbasedOptimizerConfig){
	  ldaRecommender = factorizationbasedOptimizerConfig;
  }


  public void setModelDirectory(String modelDirectory) {
    this.modelDirectory = modelDirectory;
  }

  public StorageConfiguration getStorageConfiguration() {
    return storageConfiguration;
  }

  public void setStorageConfiguration(StorageConfiguration storageConfiguration) {
    this.storageConfiguration = storageConfiguration;
  }

  public int getNumProcessorsForTraining() {
    return numProcessorsForTraining;
  }

  public void setNumProcessorsForTraining(int numProcessorsForTraining) {
    this.numProcessorsForTraining = numProcessorsForTraining;
  }
  
  public boolean getMaxPersistence(){
	  return maxPersistence;
  }

public RecommenderConfig getLDARecommender() {
	return ldaRecommender;
}
}
