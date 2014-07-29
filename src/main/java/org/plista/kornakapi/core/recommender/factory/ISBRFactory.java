package org.plista.kornakapi.core.recommender.factory;

import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.impl.recommender.AllSimilarItemsCandidateItemsStrategy;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.plista.kornakapi.KornakapiRecommender;
import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.recommender.ItemSimilarityBasedRecommender;

public class ISBRFactory {
	
	public ItemSimilarityBasedRecommender getRecommender(Configuration conf, DataModel data, String recommenderName) throws IOException{
        File modelFile = modelFile(conf, recommenderName);

        if (!modelFile.exists()) {
          boolean created = modelFile.createNewFile();
          if (!created) {
            throw new IllegalStateException("Cannot create file in model directory" + conf.getModelDirectory());
          }
        }

        ItemSimilarity itemSimilarity = new FileItemSimilarity(modelFile);
        AllSimilarItemsCandidateItemsStrategy allSimilarItemsStrategy =
            new AllSimilarItemsCandidateItemsStrategy(itemSimilarity);
        ItemSimilarityBasedRecommender recommender = new ItemSimilarityBasedRecommender(data, itemSimilarity,
            allSimilarItemsStrategy, allSimilarItemsStrategy);
        return recommender;
	}
	private File modelFile(Configuration conf, String recommenderName) {
		return new File(conf.getModelDirectory(), recommenderName + ".model");
	}

}
