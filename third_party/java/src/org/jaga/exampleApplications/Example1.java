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

public class Example1 {

	public Example1() {
	}

	public void exec() {

		GAParameterSet params = new DefaultParameterSet();
		params.setPopulationSize(0);
		params.setFitnessEvaluationAlgorithm(new Example1Fitness());
		params.setSelectionAlgorithm(new RouletteWheelSelection(-10E10));
		params.setMaxGenerationNumber(100);
		NDecimalsIndividualSimpleFactory fact = new NDecimalsIndividualSimpleFactory(2, 6, 30);
		fact.setConstraint(0, new RangeConstraint(-6, 6));
		fact.setConstraint(1, new RangeConstraint(-4, 4));
		params.setIndividualsFactory(fact);

		ReusableSimpleGA ga = new ReusableSimpleGA(params);
		AnalysisHook hook = new AnalysisHook();
		hook.setLogStream(System.out);
		hook.setUpdateDelay(100);
		hook.setAnalyseGenMinFit(true);
		ga.addHook(hook);

		final int attempts = 1;

		GAResult [] allResults = new GAResult[attempts];
		for (int i = 0; i < attempts; i++) {
			hook.reset();
			GAResult result = ((ReusableSimpleGA) ga).exec();
			allResults[i] = result;
		}
		System.out.println("\nALL DONE.\n");
		for (int i = 0; i < attempts; i++) {
			System.out.println("Result " + i + " is: " + allResults[i]);
		}

	}

	public static void main(String[] unusedArgs) {
		Example1 demo = new Example1();
		demo.exec();
	}
}