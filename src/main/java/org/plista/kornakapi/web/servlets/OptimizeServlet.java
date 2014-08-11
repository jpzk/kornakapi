package org.plista.kornakapi.web.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.plista.kornakapi.web.Parameters;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizeServlet extends BaseServlet{
	private static final Logger log = LoggerFactory.getLogger(AddCandidateServlet.class);

	  @Override
	  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

	    String label = getParameter(request, Parameters.LABEL, true);
	    String optimizerName = "factorizationbasedOptimizer_" + label;
	    if(log.isInfoEnabled()){
	    	log.info("Starting optimization for {}", label);
	    }
	    try {
			scheduler().immediatelyOptimizeRecommender(optimizerName);
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	  }
}
