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

package org.plista.kornakapi.web.servlets;

import org.plista.kornakapi.web.MissingParameterException;
import org.plista.kornakapi.web.Parameters;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/** servlet to add preferences */
public class SetPreferenceServlet extends BaseServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    long userID = getParameterAsLong(request, Parameters.USER_ID, true);
    long itemID = getParameterAsLong(request, Parameters.ITEM_ID, true);
    float value = getParameterAsFloat(request, Parameters.VALUE, true);
    String label = null;
    try{
        label = getParameter(request, Parameters.LABEL, true);
    }catch(MissingParameterException e){
    	
    }

    if(userID < 0 || userID > 2147483647){
    	userID = this.idRemapping(userID);
    }
    if(itemID < 0 || itemID > 2147483647){
    	itemID = this.idRemapping(itemID);
    }
    if(label==null){
    	label = this.getDomainIndependetStorage().getItemsLabel(itemID);
    }
    this.getDomainIndependetStorage().setPreference(userID, itemID, value);   
    preferenceChangeListener().notifyOfPreferenceChange(label);
  }
  /**
   * Method maps ids into int range
   * @param id
   * @return long
   */
  protected long idRemapping(long id){
	  return Math.abs(id % 2147483647);
  }
}
