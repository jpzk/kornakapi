package org.plista.kornakapi.core.config;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

public class FactorizationbasedOptimizerConfig extends RecommenderConfig{
	
	
	  private String optimizationDirectory;
	  private String lambdaRange;
	  private String alphaRange;
	  private String iterationRange;
	  private String featureRange;

	
	  public String getOptimizationDirectory() {
		    return optimizationDirectory;
	  }
	  
	  public ArrayList<Double> getLambdaRange(){
		  return parseDoubleRange(lambdaRange);
	  }
	  
	  public ArrayList<Double> getAlphaRange(){
		  return parseDoubleRange(alphaRange);
	  }
	  
	  public ArrayList<Integer> getIterationRange(){
		  return parseIntRange(iterationRange);
	  }
	  public ArrayList<Integer> getFeatureRange(){
		  return parseIntRange(featureRange);
	  }
	  
	  
	  
	private ArrayList<Double> parseDoubleRange(String s){
		ArrayList<Double> l = new ArrayList<Double>();
		if(s.contains("-")){
			int idx1 = s.indexOf("-");
			int idx2 = s.indexOf(",");
			double start = Double.parseDouble(s.substring(0, idx1));
			double end = Double.parseDouble(s.substring(idx1+1, idx2));
			int steps = Integer.parseInt(s.substring(idx2+1));
			double stepsize = (end - start)/steps;
			while(start < end){
				l.add(start);
				start+=stepsize;
			}
		}else if(s.contains(",")){
			String[] subs =StringUtils.split(s,",");
			for(String subss : subs){
				l.add(Double.parseDouble(subss));
			}
		}	
		return l;
	  }
	
	private ArrayList<Integer> parseIntRange(String s){
		ArrayList<Integer> l = new ArrayList<Integer>();
		if(s.contains("-")){
			int idx1 = s.indexOf("-");
			int idx2 = s.indexOf(",");
			int start = Integer.parseInt(s.substring(0, idx1));
			int end = Integer.parseInt(s.substring(idx1+1, idx2));
			int steps = Integer.parseInt(s.substring(idx2+1));
			int stepsize = (end - start)/steps;

			while(start < end){
				l.add(start);
				start+=stepsize;
			}
		}else if(s.contains(",")){
			String[] subs =StringUtils.split(s,",");
			for(String subss : subs){
				l.add(Integer.parseInt(subss));
			}
		}	
		return l;
	}
}
