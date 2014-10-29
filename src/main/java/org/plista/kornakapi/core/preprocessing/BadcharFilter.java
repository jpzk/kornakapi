package org.plista.kornakapi.core.preprocessing;

public class BadcharFilter {
	
	/**
	 * Filter bad chars
	 * 
	 * @param pText
	 */
	public String filterText(String pText) {
		return pText.replaceAll("[^A-Za-z\u00C0\u00C8\u00CC\u00D2\u00D9\u00C1\u00C9\u00CD\u00D3\u00DA\u00E0\u00E8\u00EC\u00F2\u00F9\u00E1\u00E9\u00ED\u00F3\u00FA\u00E4\u00F6\u00FC\u00C4\u00D6\u00DC\u00DF\\s]", "");
	}
}
