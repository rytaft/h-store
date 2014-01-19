package org.jaga.exampleApplications;

import org.jaga.definitions.*;
import org.jaga.util.*;
import org.jaga.masterAlgorithm.*;
import org.jaga.individualRepresentation.greycodedNumbers.*;
import org.jaga.hooks.*;
import org.jaga.selection.*;

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

public class Example2 {

	public Example2() {
	}

	public void exec() {

		GAParameterSet params = new DefaultParameterSet();
		params.setPopulationSize(100);
		params.setFitnessEvaluationAlgorithm(new Example2Fitness());
		params.setSelectionAlgorithm(new RouletteWheelSelection(-10E3));
		//params.setSelectionAlgorithm(new RouletteWheelSelection());
		params.setMaxGenerationNumber(200);

		NDecimalsIndividualSimpleFactory fact = new NDecimalsIndividualSimpleFactory(2, 0, 10);
		fact.setConstraint(0, new RangeConstraint(1, 100));
		fact.setConstraint(1, new RangeConstraint(1, 100));
		params.setIndividualsFactory(fact);

		ReusableSimpleGA ga = new ReusableSimpleGA(params);
		BetterResultHook hook = new BetterResultHook();
		ga.addHook(hook);

		final int attempts = 1;

		GAResult [] allResults = new GAResult[attempts];
		for (int i = 0; i < attempts; i++) {
			hook.resetEvaluationsCounter();
			GAResult result = ((ReusableSimpleGA) ga).exec();
			System.out.println("\nDONE.\n");
			System.out.println("Total fitness evaluations: " + hook.getFitnessEvaluations());
			allResults[i] = result;
		}
		System.out.println("\nALL DONE.\n");
		for (int i = 0; i < attempts; i++) {
			System.out.println("Result " + i + " is: " + allResults[i]);
		}

	}

	public static void main(String[] unusedArgs) {
		Example2 demo = new Example2();
		demo.exec();
	}
}