package org.plista.kornakapi.core.preprocessing;

import org.junit.Test;

import junit.framework.TestCase;

public class BadcharFilterTest extends TestCase {

	@Test
	public void testFilter() {
		BadcharFilter filter = new BadcharFilter();
		assertEquals(filter.filterText("here are som; b3d ch,,arcters"), "here are som bd charcters");
	}
}
