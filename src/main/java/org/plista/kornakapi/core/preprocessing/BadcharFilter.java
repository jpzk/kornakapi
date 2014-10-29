package org.plista.kornakapi.core.preprocessing;

public class BadcharFilter {
	
	/**
	 * Filter bad chars
	 * 
	 * @param pText
	 */
	public String filterText(String pText) {
		return pText.replaceAll("[^A-Za-zÀÈÌÒÙÁÉÍÓÚàèìòùáéíóúäöüÄÖÜß\\s]", "");
	}
}
