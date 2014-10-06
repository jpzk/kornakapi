package org.plista.kornakapi.core.storage;

import org.apache.commons.dbcp.BasicDataSource;
import org.plista.kornakapi.core.config.StorageConfiguration;

public class SemanticMySqlStorage extends MySqlStorage{
	
	

	public SemanticMySqlStorage(StorageConfiguration storageConf, String label,
			BasicDataSource dataSource) {
		super(storageConf, label, dataSource);
	    dataModel = new SemanticMySQLJDBCDataModel(dataSource, 
	    		"taste_preferences",
	            "user_id",
	            "item_id",
	            "preference",
	            "timestamp",
	            "taste_candidates",
	            "label",
	             label);
	}
}
