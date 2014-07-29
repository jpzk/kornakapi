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

/** configuration for recommenders that use matrix factorization */
public class FactorizationbasedRecommenderConfig extends RecommenderConfig {

  private boolean usesImplicitFeedback;
  private int numberOfFeatures;
  private int numberOfIterations;
  private int numberOfThreadsForEstimation;
  private double lambda;
  private double alpha;

  public int getNumberOfThreadsForEstimation() {
    return numberOfThreadsForEstimation;
  }

  public void setNumberOfThreadsForEstimation(int numberOfThreadsForEstimation) {
    this.numberOfThreadsForEstimation = numberOfThreadsForEstimation;
  }

  public boolean isUsesImplicitFeedback() {
    return usesImplicitFeedback;
  }

  public void setUsesImplicitFeedback(boolean usesImplicitFeedback) {
    this.usesImplicitFeedback = usesImplicitFeedback;
  }

  public int getNumberOfFeatures() {
    return numberOfFeatures;
  }

  public void setNumberOfFeatures(int numberOfFeatures) {
    this.numberOfFeatures = numberOfFeatures;
  }

  public int getNumberOfIterations() {
    return numberOfIterations;
  }

  public void setNumberOfIterations(int numberOfIterations) {
    this.numberOfIterations = numberOfIterations;
  }

  public double getLambda() {
    return lambda;
  }

  public void setLambda(double lambda) {
    this.lambda = lambda;
  }

  public double getAlpha() {
    return alpha;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }
}
