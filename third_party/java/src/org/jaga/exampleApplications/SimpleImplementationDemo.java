package org.jaga.exampleApplications;

import org.jaga.definitions.*;
import org.jaga.util.*;
import org.jaga.masterAlgorithm.*;
import org.jaga.individualRepresentation.greycodedNumbers.*;
import org.jaga.hooks.*;

/**
 * TODO: Complete these comments.
 *
 * <p><u>Project:</u> JAGA - Java API for Genetic Algorithms.</p>
 *
 * <p><u>Company:</u> University College London and JAGA.Org
 *    (<a href="http://www.jaga.org" target="_blank">http://www.jaga.org</a>).
 * </p>
 *
 * <p><u>Copyright:</u> (c) 2004 by G. Paperin.<br/>
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, ONLY if you include a note of the original
 *    author(s) in any redistributed/modified copy.<br/>
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.<br/>
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *    or see http://www.gnu.org/licenses/gpl.html</p>
 *
 * @author Greg Paperin (greg@jaga.org)
 *
 * @version JAGA public release 1.0 beta
 */

public class SimpleImplementationDemo {

	public SimpleImplementationDemo() {
	}

	public void exec() {

		GAParameterSet params = new DefaultParameterSet();
		params.setPopulationSize(5);
		((NDecimalsIndividualSimpleFactory) params.getIndividualsFactory()).setConstraint(0, new RangeConstraint(-10, 10));

		ReusableSimpleGA ga = new ReusableSimpleGA(params);
		ga.addHook(new DebugHook());

		int repeat = 1;
		System.out.println("\n\n");
		System.out.println("This is a simple demo for the \"Genetic Algorithms in Java\"-Package.");
		System.out.println("This software is developed by Greg Paperin at the University College London.");
		System.out.println("All materials connected to this software are under the GNU licence.");
		System.out.println("\n");
		System.out.println("Running the algorithm " + repeat + " times.");
		System.out.println("The parameters are: \n" + params);
		System.out.println("\n\n");
		for (int i = 0; i < repeat; i++) {
			System.out.println("** Run " + i + ". **");
			GAResult result = ((ReusableSimpleGA) ga).exec();
			System.out.println("Result is: " + result);
			System.out.println("\n");
		}
		System.out.println("\nDemo finished.");
		System.out.println("Please, visit http://www.jaga.org to check for latest updates.");
	}

	public static void main(String[] unusedArgs) {
		SimpleImplementationDemo demo = new SimpleImplementationDemo();
		demo.exec();

	}
}