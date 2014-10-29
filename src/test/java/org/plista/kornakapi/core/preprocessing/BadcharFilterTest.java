package org.plista.kornakapi.core.preprocessing;

import org.junit.Test;

import junit.framework.TestCase;

public class BadcharFilterTest extends TestCase {

	@Test
	public void testFilter() {
		BadcharFilter filter = new BadcharFilter();
		
		String input = new String("\u00C0\u00C8here are som; b3d ch,,arcters");
		
		assertEquals(filter.filterText("\u00C0\u00C8here are som; b3d ch,,arcters"), "\u00C0\u00C8here are som bd charcters");
	}
}
