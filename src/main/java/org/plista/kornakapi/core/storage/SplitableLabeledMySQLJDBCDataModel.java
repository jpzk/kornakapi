package org.plista.kornakapi.core.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SplitableLabeledMySQLJDBCDataModel extends LabeledMySQLJDBCDataModel{



	private final String getTrainSplitAllUsersSQL;
	private final String getTestSplitAllUsersSQL;
	private final String getNumEntrys;
	private int numEntrys;
	private final int seed;
	
	private static final Logger log = LoggerFactory.getLogger(SplitableLabeledMySQLJDBCDataModel.class);	

	public SplitableLabeledMySQLJDBCDataModel(DataSource dataSource,
			String preferenceTable, String userIDColumn, String itemIDColumn,
			String preferenceColumn, String timestampColumn,
			String candidatesTable, String labelColumn, String label, int seed) {
		
		super(dataSource, preferenceTable, userIDColumn, itemIDColumn,
				preferenceColumn, timestampColumn, candidatesTable, labelColumn, label);
		this.getNumEntrys = // getNumUsersSQL
		        "SELECT COUNT(DISTINCT " + userIDColumn +  ", " + itemIDColumn + ", " + preferenceColumn +") FROM " + preferenceTable + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"";
		try {
			numEntrys = this.getNumEntrys();
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.seed = seed;

		// get Train Set
		this.getTrainSplitAllUsersSQL =  
		        "(SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
	             + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY rand( ? ) LIMIT ?)" 
	             + " UNION ALL" + 
	             "(SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
	             + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY rand( ? ) LIMIT ?, ?)";
	    //get Train set
		this.getTestSplitAllUsersSQL = "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
	             + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY rand( ? ) LIMIT ?,?";
	}
	
	
/**

		
		// get Test Set
		this.getTrainSplitAllUsersSQL =  
		        "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
	             + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY rand(" + Integer.toString(seed) +") LIMIT " + Integer.toString(startTestSet) 
	             + "UNION" + 
	             "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
	             + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY rand(" + Integer.toString(seed) +") LIMIT " + Integer.toString(startTestSet + testBinSize) + "," + Integer.toString(numEntrys);
	       
		this.getTestSplitAllUsersSQL = "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
	             + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY rand(" + Integer.toString(seed) +") LIMIT " + Integer.toString(startTestSet) +"," + Integer.toString(testBinSize);
	
**/
	
	/**
	 * 
	 * @param fold, determines the current fold
	 * @return
	 * @throws TasteException
	 */
	public FastByIDMap<PreferenceArray> exportTrainSetWithPrefs(int fold) throws TasteException {
		
		int testBinSize = (int) (numEntrys * 0.3);
		
		int startTestSet = testBinSize * fold;
		
	    log.debug("Exporting all data");

	    PreparedStatement stmt = null;
	    Connection conn = null;
	    ResultSet rs = null;

	    FastByIDMap<PreferenceArray> result = new FastByIDMap<PreferenceArray>();

	    try {
	    	conn = getDataSource().getConnection();
	        stmt = conn.prepareStatement(getTrainSplitAllUsersSQL);
	        stmt.setLong(1, seed);
	        stmt.setLong(2, startTestSet);
	        stmt.setLong(3, seed);
	        stmt.setLong(4, startTestSet + testBinSize);
	        stmt.setLong(5, numEntrys);
	        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
	        stmt.setFetchSize(getFetchSize());
			log.debug("Executing SQL query: {}", getTrainSplitAllUsersSQL);
			rs = stmt.executeQuery();

			Long currentUserID = null;
			List<Preference> currentPrefs = Lists.newArrayList();
			while (rs.next()) {
				long nextUserID = getLongColumn(rs, 1);
		        if (currentUserID != null && !currentUserID.equals(nextUserID) && !currentPrefs.isEmpty()) {
		          result.put(currentUserID, new GenericUserPreferenceArray(currentPrefs));
		          currentPrefs.clear();
		        }
		        currentPrefs.add(buildPreference(rs));
		        currentUserID = nextUserID;
			}
			if (!currentPrefs.isEmpty()) {
				result.put(currentUserID, new GenericUserPreferenceArray(currentPrefs));
			}

			return result;

	    } catch (SQLException sqle) {
	      log.warn("Exception while exporting all data", sqle);
	      throw new TasteException(sqle);
	    } finally {
	      IOUtils.quietClose(rs, stmt, conn);

	    }
	  }
	  /**
	   * 
	   * @param fold, determines the current fold
	   * @return
	   * @throws TasteException
	   */
	  public FastByIDMap<PreferenceArray> exportTestSetWithPrefs(int fold) throws TasteException {
			
			int testBinSize = (int) (numEntrys * 0.3);
			
			int startTestSet = testBinSize * fold;

		    Connection conn = null;
		    PreparedStatement stmt = null;
		    ResultSet rs = null;

		    FastByIDMap<PreferenceArray> result = new FastByIDMap<PreferenceArray>();

		    try {
		    	conn = getDataSource().getConnection();
		        stmt = conn.prepareStatement(getTestSplitAllUsersSQL);
		        stmt.setLong(1, seed);
		        stmt.setLong(2, startTestSet);
		        stmt.setLong(3, testBinSize);
		        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
		        stmt.setFetchSize(getFetchSize());

		        log.debug("Executing SQL query: {}", getTestSplitAllUsersSQL);
		        rs = stmt.executeQuery();

		        Long currentUserID = null;
		        List<Preference> currentPrefs = Lists.newArrayList();
		        while (rs.next()) {
		        	long nextUserID = getLongColumn(rs, 1);
		        	if (currentUserID != null && !currentUserID.equals(nextUserID) && !currentPrefs.isEmpty()) {
		        		result.put(currentUserID, new GenericUserPreferenceArray(currentPrefs));
		        		currentPrefs.clear();
		        	}
		        	currentPrefs.add(buildPreference(rs));
		        	currentUserID = nextUserID;
		        }
		        if (!currentPrefs.isEmpty()) {
		        	result.put(currentUserID, new GenericUserPreferenceArray(currentPrefs));
		        }

		        return result;

		    } catch (SQLException sqle) {
		      log.warn("Exception while exporting all data", sqle);
		      throw new TasteException(sqle);
		    } finally {
		      IOUtils.quietClose(rs, stmt, conn);

		    }
		  }
	  
	  private int getNumEntrys() throws TasteException {
		    log.debug("Retrieving number of entrys in db");
		    Connection conn = null;
		    PreparedStatement stmt = null;
		    ResultSet rs = null;
		    try {
		      conn = getDataSource().getConnection();
		      stmt = conn.prepareStatement(this.getNumEntrys, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
		      stmt.setFetchSize(getFetchSize());
		      log.debug("Executing SQL query: {}", this.getNumEntrys);
		      rs = stmt.executeQuery();
		      rs.next();
		      return rs.getInt(1);
		    } catch (SQLException sqle) {
		      log.warn("Exception while retrieving number of entrys");
		      throw new TasteException(sqle);
		    } finally {
		      IOUtils.quietClose(rs, stmt, conn);
		    }
		  }

}
