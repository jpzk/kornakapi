package org.plista.kornakapi.core.storage;

import javax.sql.DataSource;

public class SemanticLabeledMySQLJDBCDataModel extends LabeledMySQLJDBCDataModel {
	/**
	 * Only taste_candidates is used here
	 * @param dataSource
	 * @param preferenceTable
	 * @param userIDColumn
	 * @param itemIDColumn
	 * @param preferenceColumn
	 * @param timestampColumn
	 * @param candidatesTable
	 * @param labelColumn
	 * @param label
	 */
	public SemanticLabeledMySQLJDBCDataModel(DataSource dataSource,
			String preferenceTable,  String userIDColumn, String itemIDColumn,
			String preferenceColumn, String timestampColumn, String candidatesTable,String labelColumn, String label) {
		
		
		
		super(dataSource, preferenceTable, userIDColumn, itemIDColumn,
				preferenceColumn, timestampColumn,     
				// getPreferenceSQL
			    "SELECT " + preferenceColumn + " FROM " + preferenceTable + " WHERE " + userIDColumn + "=? AND "
		        + itemIDColumn + "=?" + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"",
		    // getPreferenceTimeSQL
		    "SELECT " + timestampColumn + " FROM " + preferenceTable + " WHERE " + userIDColumn + "=? AND "
		        + itemIDColumn + "=?",
		    // getUserSQL
		    "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
		         + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " && " + userIDColumn + "=? " + " ORDER BY " + itemIDColumn,
		    // getAllUsersSQL
		    "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
		         + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY " + userIDColumn + ", " + itemIDColumn,
		    // getNumItemsSQL
		         "SELECT COUNT(DISTINCT " + itemIDColumn + ") FROM " + candidatesTable + " WHERE " +labelColumn +"="+ "\""+label + "\"",
		    // getNumUsersSQL
		    "SELECT COUNT(DISTINCT " + userIDColumn + ") FROM " + preferenceTable + " INNER JOIN " + candidatesTable + " c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"",
		    // setPreferenceSQL
		    "INSERT INTO " + preferenceTable + '(' + userIDColumn + ',' + itemIDColumn + ',' + preferenceColumn
		        + ") VALUES (?,?,?) ON DUPLICATE KEY UPDATE " + preferenceColumn + "=?",
		    // removePreference SQL
		    "DELETE FROM " + preferenceTable + " WHERE " + userIDColumn + "=? AND " + itemIDColumn + "=?",
		    // getUsersSQL
		    "SELECT DISTINCT " + userIDColumn + " FROM (SELECT " + userIDColumn +", " + itemIDColumn +" FROM " + preferenceTable + " INNER JOIN " + candidatesTable +" c USING ("+ itemIDColumn + ") WHERE c." +labelColumn +"="+ "\""+label + "\"" + " ORDER BY " + userIDColumn + ") as t",
		    // getItemsSQL
		    "SELECT DISTINCT " + itemIDColumn + " FROM "+ candidatesTable + " WHERE " +labelColumn +"="+ "\""+label + "\"" + " ORDER BY " + itemIDColumn,
		    // getPrefsForItemSQL
		    "SELECT DISTINCT " + userIDColumn + ", " + itemIDColumn + ", " + preferenceColumn + " FROM " + preferenceTable
		        + " WHERE " + itemIDColumn + "=? ORDER BY " + userIDColumn,
		    // getNumPreferenceForItemSQL
		    "SELECT COUNT(1) FROM " + preferenceTable + " WHERE " + itemIDColumn + "=?",
		    // getNumPreferenceForItemsSQL
		    "SELECT COUNT(1) FROM " + preferenceTable + " tp1 JOIN " + preferenceTable + " tp2 " + "USING ("
		        + userIDColumn + ") WHERE tp1." + itemIDColumn + "=? and tp2." + itemIDColumn + "=?",
		    "SELECT MAX(" + preferenceColumn + ") FROM " + preferenceTable,
		    "SELECT MIN(" + preferenceColumn + ") FROM " + preferenceTable);
	}
    


		
		
	



}
