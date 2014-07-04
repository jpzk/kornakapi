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

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.MostSimilarItemsCandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Rescorer;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.common.LongPair;
import org.plista.kornakapi.KornakapiRecommender;

import java.util.List;

/** an item kNN recommender */
public class ItemSimilarityBasedRecommender extends GenericItemBasedRecommender implements KornakapiRecommender {

  public ItemSimilarityBasedRecommender(DataModel dataModel, ItemSimilarity similarity,
      CandidateItemsStrategy candidateItemsStrategy,
      MostSimilarItemsCandidateItemsStrategy mostSimilarItemsCandidateItemsStrategy) {
    super(dataModel, similarity, candidateItemsStrategy, mostSimilarItemsCandidateItemsStrategy);
  }

  @Override
  public List<RecommendedItem> recommendToAnonymous(long[] itemIDs, int howMany, IDRescorer idRescorer)
      throws TasteException {
    Rescorer<LongPair> rescorer = idRescorer != null ? new FilteringLongPairRescorer(idRescorer) : null;

    return mostSimilarItems(itemIDs, howMany, rescorer, false);
  }

@Override
public List<RecommendedItem> recommend(long userID, long[] itemIDs,
		int howMany, IDRescorer rescorer) throws TasteException {
	// TODO Auto-generated method stub
	return null;
}
}
