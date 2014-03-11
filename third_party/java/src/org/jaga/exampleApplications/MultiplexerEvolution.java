package org.jaga.exampleApplications;

import org.jaga.individualRepresentation.booleanFormulas.*;
import org.jaga.reproduction.booleanFormulas.nodes.*;
import org.jaga.definitions.*;
import org.jaga.util.*;
import org.jaga.masterAlgorithm.*;
import org.jaga.reproduction.*;
import org.jaga.hooks.*;
import org.jaga.fitnessEvaluation.multiplexer.*;
import org.jaga.selection.*;
import org.jaga.reproduction.booleanFormulas.*;


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
public class MultiplexerEvolution {

	public MultiplexerEvolution() {}

	public void exec() {

		GAParameterSet params = new DefaultParameterSet();
		params.setPopulationSize(1000);
		Multiplexer multiplexer = new Multiplexer(8);
		params.setFitnessEvaluationAlgorithm(new MultiplexerFitness(multiplexer, 1.7));
		CombinedReproductionAlgorithm repAlg = new CombinedReproductionAlgorithm();
		repAlg.insertReproductionAlgorithm(0, new FunctionTreeXOver(0.8));
		repAlg.insertReproductionAlgorithm(1, new FunctionTreeMutation(0.15));
		params.setReproductionAlgorithm(repAlg);
		params.setMaxGenerationNumber(300);
		params.setSelectionAlgorithm(new TournamentSelection(6, 0.9));
					//new RouletteWheelSelection(-5));
		BooleanFormulaTreeFactory factory = new BooleanFormulaTreeFactory();
		factory.setAllowConstants(false);
		factory.setMaxTreeDepth(6);
		factory.setNumberOfParameters(11);
		final int attempts = 1;

		factory.setAllowedNodeTypes(new Class[] { TerminalNode.class,
												  //ANDNode.class,
												  //ORNode.class,
												  //NOTNode.class,
												  //NANDNode.class,
												  //NORNode.class,
												  //XORNode.class,
												 IFNode.class,
												 //EQUIVNode.class,
												 //IMPLNode.class,
												 });

		params.setIndividualsFactory(factory);

		//ReusableSimpleGA ga = new ReusableSimpleGA(params);
		ElitistGA ga = new ElitistGA(params, 0.2, 0.1);   // params, elite, bad
		AnalysisHook hook = new AnalysisHook();
		hook.setLogStream(System.out);
		hook.setUpdateDelay(1500);
		ga.addHook(hook);
		GAResult [] allResults = new GAResult[attempts];

		for (int i = 0; i < attempts; i++) {

			System.out.println("\n ========== STARTING RUN " + (i+1) + ". ==============================\n");
			hook.reset();

			GAResult result = new FittestIndividualResult();
			try {
				result = ((ReusableSimpleGA) ga).exec();
			} catch (OutOfMemoryError e) {
				e.printStackTrace();
			}

			System.out.println("\nDONE.\n");
			System.out.println("Total fitness evaluations: " + hook.getFitnessCalculations());
			System.out.println("Result is: " + result);
			System.out.println("(Fitness: " + ((FittestIndividualResult) result).getBestFitness() + ")");
			allResults[i] = result;
			factory.setAllowConstants(!factory.getAllowConstants());
		}

		System.out.println("\nALL DONE.\n");
		for (int i = 0; i < attempts; i++) {
			System.out.println("Result " + i + " is: " + allResults[i]);
			System.out.println("(Fitness " + i + " is:" + ((FittestIndividualResult) allResults[i]).getBestFitness() + ")");
		}

	}

	public static void main(String [] unusedArgs) {
		MultiplexerEvolution multiplexerEvolution = new MultiplexerEvolution();
		multiplexerEvolution.exec();
	}

}
