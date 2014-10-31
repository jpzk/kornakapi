package org.plista.kornakapi.core.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.plista.kornakapi.core.config.Configuration;
import org.plista.kornakapi.core.config.LDARecommenderConfig;
import org.plista.kornakapi.core.storage.CandidateCacheStorageDecorator;
import org.plista.kornakapi.web.Components;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LDAArticleWriter {
	
	  private static final Logger log = LoggerFactory.getLogger(LDAArticleWriter.class);
	public void writeArticle(String pLabel, long pItemId, String pText, String pTSet) throws IOException {
		Components components = Components.instance();
		Configuration config = components.getConfiguration();
		HashMap<String, CandidateCacheStorageDecorator> storages = components.storages();
		
    	String path = ((LDARecommenderConfig) config.getLDARecommender()).getInferencePath()+ "Documents/" + pTSet + "/" + Long.toString(pItemId);
    	log.warn(path);
    	File f = new File(path);
    	if(!f.exists()){
    		f.createNewFile();
    		BufferedWriter output = new BufferedWriter(new FileWriter(f));
            output.write(pText);
            output.close();
    	}
	}
}
