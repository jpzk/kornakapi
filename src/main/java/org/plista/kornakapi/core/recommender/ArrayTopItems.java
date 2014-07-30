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

import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class ArrayTopItems {

  public static List<RecommendedItem> getTopItems(int howMany, long[] possibleItemIDs, int fromIndex, int toIndex,
    IDRescorer rescorer, TopItems.Estimator<Long> estimator) throws TasteException {

    Preconditions.checkArgument(possibleItemIDs != null, "possibleItemIDs is null");
    Preconditions.checkArgument(estimator != null, "estimator is null");

    Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(howMany + 1,
        Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
    boolean full = false;
    double lowestTopValue = Double.NEGATIVE_INFINITY;
    for (int index = fromIndex; index < toIndex; index++) {
      long itemID = possibleItemIDs[index];
      if (rescorer == null || !rescorer.isFiltered(itemID)) {
        double preference;
        try {
          preference = estimator.estimate(itemID);
        } catch (NoSuchItemException nsie) {
          continue;
        } catch (NoSuchUserException nsue){
          continue;
        }
        double rescoredPref = rescorer == null ? preference : rescorer.rescore(itemID, preference);
        if (!Double.isNaN(rescoredPref) && (!full || rescoredPref > lowestTopValue)) {
          topItems.add(new GenericRecommendedItem(itemID, (float) rescoredPref));
          if (full) {
            topItems.poll();
          } else if (topItems.size() > howMany) {
            full = true;
            topItems.poll();
          }
          lowestTopValue = topItems.peek().getValue();
        }
      }
    }
    int size = topItems.size();
    if (size == 0) {
      return Collections.emptyList();
    }
    List<RecommendedItem> result = Lists.newArrayListWithCapacity(size);
    result.addAll(topItems);
    Collections.sort(result, ByValueRecommendedItemComparator.getInstance());
    return result;
  }
}
