/**
 * Copyright 2014 plista GmbH  (http://www.plista.com/)
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParallelTopItems {

  public static List<RecommendedItem> getTopItems(int howMany, int numThreads, FastIDSet possibleItemIDs,
      final IDRescorer rescorer, final TopItems.Estimator<Long> estimator) throws TasteException {

    Preconditions.checkArgument(numThreads > 2);
    Preconditions.checkNotNull(possibleItemIDs);
    Preconditions.checkNotNull(estimator);

    //long s = System.currentTimeMillis();

    long[] itemIDsToEstimate = possibleItemIDs.toArray();

    //long d = System.currentTimeMillis() - s;
    //System.out.println("Preparation " + d);

    //s = System.currentTimeMillis();
    ExecutorService queue = Executors.newFixedThreadPool(numThreads);

    int fromIndex = 0;
    List<EstimationWorker> estimatorWorkers = Lists.newArrayListWithCapacity(numThreads);
    for (int n = 0; n < numThreads; n++) {

      int toIndex = Math.min(fromIndex + itemIDsToEstimate.length / numThreads, itemIDsToEstimate.length);
      EstimationWorker worker = new EstimationWorker(howMany, itemIDsToEstimate, fromIndex, toIndex, rescorer,
                                                     estimator);
      estimatorWorkers.add(worker);
      queue.execute(worker);
      fromIndex = toIndex;
    }

    queue.shutdown();
    try {
      queue.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    //d = System.currentTimeMillis() - s;
    //System.out.println("Estimation " + d);

    List<RecommendedItem> topItems = Lists.newArrayList();
    for (int n = 0; n < numThreads; n++) {
      topItems.addAll(estimatorWorkers.get(n).topItems);
    }

    if (topItems.isEmpty()) {
      return Collections.EMPTY_LIST;
    }
    if(topItems.size() < howMany){
    	howMany = topItems.size();
    }
    Collections.sort(topItems, ByValueRecommendedItemComparator.getInstance());
    List<RecommendedItem> recommendedItems = topItems.subList(0, howMany);

    return recommendedItems;
  }

  static class EstimationWorker implements Runnable {

    private final int howMany;
    private final long[] possibleItemIDs;
    private final int fromIndex;
    private final int toIndex;
    private final IDRescorer rescorer;
    private final TopItems.Estimator<Long> estimator;

    private List<RecommendedItem> topItems;

    EstimationWorker(int howMany, long[] possibleItemIDs, int fromIndex, int toIndex, IDRescorer rescorer,
                     TopItems.Estimator<Long> estimator) {
      this.howMany = howMany;
      this.possibleItemIDs = possibleItemIDs;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
      this.rescorer = rescorer;
      this.estimator = estimator;
    }

    @Override
    public void run() {
      try {
        topItems = ArrayTopItems.getTopItems(howMany, possibleItemIDs, fromIndex, toIndex, rescorer, estimator);
      } catch (TasteException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
