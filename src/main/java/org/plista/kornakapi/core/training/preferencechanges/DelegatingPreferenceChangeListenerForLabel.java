package org.plista.kornakapi.core.training.preferencechanges;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DelegatingPreferenceChangeListenerForLabel implements PreferenceChangeListenerForLabel {
	  private final Map<String,List<PreferenceChangeListener>> delegates = Maps.newLinkedHashMap();

	  public void addDelegate(PreferenceChangeListener listener, String label) {
		  if(delegates.containsKey(label)){
			  delegates.get(label).add(listener);
		  }else{
			  List<PreferenceChangeListener> delegatesPerLabel = Lists.newArrayList();
			  delegatesPerLabel.add(listener);
			  delegates.put(label, delegatesPerLabel);
		  }
	    
	  }

	  @Override
	  public void notifyOfPreferenceChange(String label) {
	    for (PreferenceChangeListener listener : delegates.get(label)) {
	      listener.notifyOfPreferenceChange();
	    }
	  }
	}
