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

package org.plista.kornakapi.core.storage;

import com.google.common.collect.Sets;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.JDBCDataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.common.IOUtils;
import org.plista.kornakapi.core.Candidate;
import org.plista.kornakapi.core.config.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/** an implementation of {@link Storage} for MySQL
 *
 *

 CREATE TABLE taste_preferences(
    user_id bigint(20) not null,
    item_id bigint(20) not null,
    preference float not null,
    timeframe bit(4) NOT NULL DEFAULT 0,
    primary key( user_id, item_id, timeframe),
    key item_id(item_id)
 )
 partition by range(timeframe) (
    PARTITION p0 VALUES LESS THAN (1),
    PARTITION p1 VALUES LESS THAN (2),
    PARTITION p2 VALUES LESS THAN (3),
    PARTITION p3 VALUES LESS THAN (4),
    PARTITION p4 VALUES LESS THAN (5),
    PARTITION p5 VALUES LESS THAN (6)
 );

 CREATE TABLE taste_candidates (
   label varchar(255) NOT NULL,
   item_id bigint(20) NOT NULL,
   PRIMARY KEY (label,item_id)
 );
 


 **/
public class MySqlStorage implements Storage {

  protected final BasicDataSource dataSource;
  protected JDBCDataModel dataModel;
  private int timeWindow;

  private static final String IMPORT_QUERY =
      "INSERT INTO taste_preferences (user_id, item_id, preference) VALUES (?, ?, ?) " +
      "ON DUPLICATE KEY UPDATE preference = VALUES(preference)";

  private static final String INSERT_CANDIDATE_QUERY =
      "INSERT INTO taste_candidates (label, item_id) VALUES (?, ?)";

  private static final String REMOVE_CANDIDATE_QUERY =
      "DELETE FROM taste_candidates WHERE label = ? AND item_id = ?";

  private static final String REMOVE_ALL_CANDIDATES_QUERY =
      "DELETE FROM taste_candidates WHERE label = ?";

  private static final String GET_CANDIDATES_QUERY =
      "SELECT item_id FROM taste_candidates WHERE label = ?";
  
  private static final String GET_LABELS = "SELECT DISTINCT label FROM taste_candidates";
  
  private static final String GET_ITEMSLABEL = "SELECT label FROM taste_candidates WHERE item_id = ?";

  private static final String SWITCH_CURRENT_PARTITION_QUERY =
      "ALTER TABLE taste_preferences MODIFY timeframe INT DEFAULT ?;";

  private static final String TRUNCATE_PREVIOUS_PARTITION_QUERY_FRAGMENT =
      "ALTER TABLE taste_preferences TRUNCATE PARTITION ";

  private static final Logger log = LoggerFactory.getLogger(MySqlStorage.class);

  public MySqlStorage(StorageConfiguration storageConf, String label, BasicDataSource dataSource) {


    dataSource.setDriverClassName(storageConf.getJdbcDriverClass());
    dataSource.setUrl(storageConf.getJdbcUrl());
    dataSource.setUsername(storageConf.getUsername());
    dataSource.setPassword(storageConf.getPassword());

    //TODO should be made configurable
    dataSource.setMaxActive(10);
    dataSource.setMinIdle(5);
    dataSource.setInitialSize(5);
    dataSource.setValidationQuery("SELECT 1;");
    dataSource.setTestOnBorrow(false);
    dataSource.setTestOnReturn(false);
    dataSource.setTestWhileIdle(true);
    dataSource.setTimeBetweenEvictionRunsMillis(5000);

    dataModel = new LabeledMySQLJDBCDataModel(dataSource, 
    		"taste_preferences",
            "user_id",
            "item_id",
            "preference",
            "timestamp",
            "taste_candidates",
            "label",
             label);
    this.dataSource = dataSource;
    this.timeWindow = storageConf.getTimeWindow();
    if(timeWindow % 6 !=0 || timeWindow == 0){
    	timeWindow = 24;
    }

  }

  @Override
  public DataModel trainingData() throws IOException {
    try {
      return new GenericDataModel(dataModel.exportWithPrefs());
    } catch (TasteException e) {
      throw new IOException(e);
    }
  }

  @Override
  public DataModel recommenderData() throws IOException {
    return dataModel;
  }

  @Override
  public void setPreference(long userID, long itemID, float value) throws IOException {
    try {
      dataModel.setPreference(userID, itemID, value);
    } catch (TasteException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void batchSetPreferences(Iterator<Preference> preferences, int batchSize) throws IOException {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(IMPORT_QUERY);

      int recordsQueued = 0;

      while (preferences.hasNext()) {
        Preference preference = preferences.next();
        stmt.setLong(1, preference.getUserID());
        stmt.setLong(2, preference.getItemID());
        stmt.setFloat(3, preference.getValue());
        stmt.addBatch();

        if (++recordsQueued % batchSize == 0) {
          stmt.executeBatch();
          log.info("imported {} records in batch", recordsQueued);
        }
      }

      if (recordsQueued % batchSize != 0) {
        stmt.executeBatch();
        log.info("imported {} records in batch. done.", recordsQueued);
      }

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(stmt);
      IOUtils.quietClose(conn);
    }
  }

  /**
   port from PHP code:

   $hours=  (int)(time()/3600)  ;
   $selDay=  $hours%  72;
   $selHalfDay=  (int)($selDay/  12);
   $sql=  'alter table taste_preferences modify timeframe int default'. $selHalfDay;
   $stmt=  new  PDO('mysql:host=localhost;dbname=kornakapi;charset=utf8','dbname','dbpwd');
   $stmt->exec($sql);
   $olttable=  ($selHalfDay+1)  %  6;
   $sql=  'ALTER TABLE taste_preferences TRUNCATE PARTITION p'.$olttable;
   $stmt=  new  PDO('mysql:host=localhost;dbname=kornakapi;charset=utf8','dbname','dbpwd');
   $stmt->exec($sql);

   * @throws IOException
   */
  @Override
  public void purgeOldPreferences() throws IOException {
    Connection conn = null;
    PreparedStatement switchStmt = null;
    Statement truncateStmt = null;

    try {
      conn = dataSource.getConnection();

      long hours = System.currentTimeMillis() / (3600 * 1000);
      int selDay = (int) (hours % timeWindow);
      int selfHalfDay = selDay / (timeWindow / 6);

      switchStmt = conn.prepareStatement(SWITCH_CURRENT_PARTITION_QUERY);
      switchStmt.setInt(1, selfHalfDay);
      switchStmt.execute();

      int indexOfPartitionToPurge = (selfHalfDay + 1) % 6;

      truncateStmt = conn.createStatement();
      truncateStmt.execute(TRUNCATE_PREVIOUS_PARTITION_QUERY_FRAGMENT +
                           "p" + String.valueOf(indexOfPartitionToPurge) + ";");

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(switchStmt);
      IOUtils.quietClose(truncateStmt);
      IOUtils.quietClose(conn);
    }
  }

  @Override
  public void addCandidate(String label, long itemID) throws IOException {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(INSERT_CANDIDATE_QUERY);

      stmt.setString(1, label);
      stmt.setLong(2, itemID);

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

  @Override
  public Iterable<String> batchAddCandidates(Iterator<Candidate> candidates, int batchSize) throws IOException {

    Set<String> modifiedLabels = Sets.newHashSet();

    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(INSERT_CANDIDATE_QUERY);

      int recordsQueued = 0;

      while (candidates.hasNext()) {

        Candidate candidate = candidates.next();

        modifiedLabels.add(candidate.getLabel());

        stmt.setString(1, candidate.getLabel());
        stmt.setLong(2, candidate.getItemID());
        stmt.addBatch();

        if (++recordsQueued % batchSize == 0) {
          stmt.executeBatch();
          log.info("imported {} candidates in batch", recordsQueued);
        }
      }

      if (recordsQueued % batchSize != 0) {
        stmt.executeBatch();
        log.info("imported {} candidates in batch. done.", recordsQueued);
      }

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(stmt);
      IOUtils.quietClose(conn);
    }

    return modifiedLabels;
  }

  @Override
  public void deleteCandidate(String label, long itemID) throws IOException {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(REMOVE_CANDIDATE_QUERY);

      stmt.setString(1, label);
      stmt.setLong(2, itemID);

      stmt.execute();

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(stmt);
      IOUtils.quietClose(conn);
    }
  }

  @Override
  public void deleteAllCandidates(String label) throws IOException {
    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(REMOVE_ALL_CANDIDATES_QUERY);

      stmt.setString(1, label);

      stmt.execute();

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(stmt);
      IOUtils.quietClose(conn);
    }
  }

  @Override
  public Iterable<String> batchDeleteCandidates(Iterator<Candidate> candidates, int batchSize) throws IOException {

    Set<String> modifiedLabels = Sets.newHashSet();

    Connection conn = null;
    PreparedStatement stmt = null;

    try {
      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(REMOVE_CANDIDATE_QUERY);

      int recordsQueued = 0;

      while (candidates.hasNext()) {

        Candidate candidate = candidates.next();

        modifiedLabels.add(candidate.getLabel());

        stmt.setString(1, candidate.getLabel());
        stmt.setLong(2, candidate.getItemID());
        stmt.addBatch();

        if (++recordsQueued % batchSize == 0) {
          stmt.executeBatch();
          log.info("deleted {} candidates in batch", recordsQueued);
        }
      }

      if (recordsQueued % batchSize != 0) {
        stmt.executeBatch();
        log.info("deleted {} candidates in batch. done.", recordsQueued);
      }

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(stmt);
      IOUtils.quietClose(conn);
    }

    return modifiedLabels;
  }

  @Override
  public FastIDSet getCandidates(String label) throws IOException {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {

      FastIDSet candidates = new FastIDSet();

      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(GET_CANDIDATES_QUERY, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
      stmt.setFetchSize(1000);
      stmt.setString(1, label);

      rs = stmt.executeQuery();

      while (rs.next()) {
        candidates.add(rs.getLong(1));
      }

      return candidates;

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(rs, stmt, conn);
    }
  }
  
  public LinkedList<String> getAllLabels() throws IOException {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;

    try {

      LinkedList<String> candidates = new LinkedList<String>();

      conn = dataSource.getConnection();
      stmt = conn.prepareStatement(GET_LABELS, ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
      stmt.setFetchSize(1000);
      rs = stmt.executeQuery();

      while (rs.next()) {
        candidates.add(rs.getString(1));
      }

      return candidates;

    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      IOUtils.quietClose(rs, stmt, conn);
    }
  }
  
  public String getItemsLabel(long itemid) throws IOException {
	    Connection conn = null;
	    PreparedStatement stmt = null;
	    ResultSet rs = null;

	    try {

	      conn = dataSource.getConnection();
	      stmt = conn.prepareStatement(GET_ITEMSLABEL, ResultSet.TYPE_FORWARD_ONLY,
	          ResultSet.CONCUR_READ_ONLY);
	      stmt.setLong(1, itemid);
	      stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
	      stmt.setFetchSize(10);
	      rs = stmt.executeQuery();
	      String label = null;
	      if(rs.next()){
	    	  label = rs.getString(1);
	      }

	      return label;

	    } catch (SQLException e) {
	      throw new IOException(e);
	    } finally {
	      IOUtils.quietClose(rs, stmt, conn);
	    }
	  }

  @Override
  public void close() throws IOException {
    try {
      dataSource.close();
    } catch (SQLException e) {
      throw new IOException("Unable to close datasource", e);
    }
  }
}
