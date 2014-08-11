package org.plista.kornakapi.core.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.common.IOUtils;
import org.plista.kornakapi.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
CREATE TABLE taste_optimization(
	label varchar(255),
	features int,
	iterations int,
	alpha double ,
	lambda double,
	error double
);

 
 */

public class MySqlSplitableMaxPersistentStorage extends MySqlMaxPersistentStorage implements Storage{
	
	  protected final SplitableLabeledMySQLJDBCDataModel dataModel;
	  
	  private static final String INSERT_PERFORMANCE ="INSERT INTO taste_optimization (label, features, iterations, alpha, lambda, error) VALUES (?, ?, ? , ?, ?, ?)";
	  
	  private static final Logger log = LoggerFactory.getLogger(MySqlSplitableMaxPersistentStorage.class);

	public MySqlSplitableMaxPersistentStorage(StorageConfiguration storageConf,
			String label, BasicDataSource dataSource, int seed) {
		super(storageConf, label, dataSource);
	    dataModel = new SplitableLabeledMySQLJDBCDataModel(dataSource, 
	    		"taste_preferences",
	            "user_id",
	            "item_id",
	            "preference",
	            "timestamp",
	            "taste_candidates",
	            "label",
	             label, seed);
		// TODO Auto-generated constructor stub
	}  
	  public DataModel trainingData(int split) throws IOException {
	    try {
	      return new GenericDataModel(dataModel.exportTrainSetWithPrefs(split));
	    } catch (TasteException e) {
	      throw new IOException(e);
	    }
	  }
	  
	  public DataModel testData(int split) throws IOException{
		    try {
			      return new GenericDataModel(dataModel.exportTestSetWithPrefs(split));
			    } catch (TasteException e) {
			      throw new IOException(e);
			    }
	  }
	  
	  public void insertPerformance(String label, int  features, int iterations, double alpha, double lambda, double error) throws IOException {
		    Connection conn = null;
		    PreparedStatement stmt = null;

		    try {
		      conn = dataSource.getConnection();
		      stmt = conn.prepareStatement(INSERT_PERFORMANCE);

		      stmt.setString(1, label);
		      stmt.setInt(2, features);
		      stmt.setInt(3, iterations);
		      stmt.setDouble(4, alpha);
		      stmt.setDouble(5, lambda);
		      stmt.setDouble(6, error);

		      stmt.execute();

		    } catch (SQLException e) {
			    if (log.isInfoEnabled()) {
			    	log.info(e.getMessage()); 			    			
			    }else{
			    	throw new IOException(e);
			    }
		      
		    } finally {
		      IOUtils.quietClose(stmt);
		      IOUtils.quietClose(conn);
		    }
		  }

}
