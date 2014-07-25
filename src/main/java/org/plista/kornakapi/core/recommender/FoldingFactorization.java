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

package org.plista.kornakapi.core.recommender;

import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.linear.LUDecompositionImpl;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** a matrix factorization that supports folding in new users */
public class FoldingFactorization {

  private final Factorization factorization;
  private final double[][] userFoldInMatrix;

  private static final Logger log = LoggerFactory.getLogger(FoldingFactorization.class);

  public FoldingFactorization(Factorization factorization) {
    this.factorization = factorization;
    userFoldInMatrix = computeUserFoldInMatrix(factorization.allItemFeatures());
  }

  public Factorization factorization() {
    return factorization;
  }

  /* see http://www.slideshare.net/fullscreen/srowen/matrix-factorization/16 for details */
  private double[][] computeUserFoldInMatrix(double[][] itemFeatures) {

    /* if there are no items, we cannot fold in anything */
    if (itemFeatures.length == 0) {
      return new double[0][0];
    }

    if (log.isInfoEnabled()) {
      log.info("Computing fold-in matrix from a {} x {} item features matrix", factorization.numItems(),
          factorization.numFeatures());
    }

    RealMatrix Y = new Array2DRowRealMatrix(itemFeatures);
    RealMatrix YTY = Y.transpose().multiply(Y);
    RealMatrix YTYInverse = new LUDecompositionImpl(YTY).getSolver().getInverse();

    return Y.multiply(YTYInverse).getData();
  }

  public double[] foldInUser(long[] itemIDs) throws NoSuchItemException {

    double[] userFeatures = new double[factorization.numFeatures()];
    for (long itemID : itemIDs) {
    	int itemIndex = -1;
    	try{
	      itemIndex = factorization.itemIndex(itemID);
    	}catch(NoSuchItemException e){
    	    if (log.isDebugEnabled()) {
    	        log.debug("Item unknown: {}", itemID);
    	        if(itemIDs.length == 1){
    	        	throw new NoSuchItemException("At least one item must be known");
    	        }
    	    }
    	}
    	if(itemIndex >=0){
    	    for (int feature = 0; feature < factorization.numFeatures(); feature++) {
    	        userFeatures[feature] += userFoldInMatrix[itemIndex][feature];
    	    }
    	}
    }
    return userFeatures;
  }
  
  public double[] foldInAnonymousUser(long[] itemIDs) throws NoSuchItemException {

	    double[] userFeatures = new double[factorization.numFeatures()];
	    for (long itemID : itemIDs) {
	    	try{
		      int itemIndex = factorization.itemIndex(itemID);
		      double fnorm = 0;
		      for (int feature = 0; feature < factorization.numFeatures(); feature++) {
		    	  fnorm += factorization.allItemFeatures()[itemIndex][feature] * factorization.allItemFeatures()[itemIndex][feature];
		      }
		      fnorm = Math.sqrt(fnorm);
		      for (int feature = 0; feature < factorization.numFeatures(); feature++) {
		        userFeatures[feature] += factorization.allItemFeatures()[itemIndex][feature]/fnorm;
		      }
	    	}catch(NoSuchItemException e){
	    	    if (log.isDebugEnabled()) {
	    	        log.debug("Item unknown: {}", itemID);
	    	        if(itemIDs.length == 1){
	    	        	throw new NoSuchItemException("At least one item must be known");
	    	        }
	    	    }
	    	}
	    }
	    RealVector  userFeaturesAsVector = new ArrayRealVector(userFeatures);
	    RealVector normalised =  userFeaturesAsVector.mapDivide(userFeaturesAsVector.getL1Norm());

	    return normalised.getData();
  }
}
