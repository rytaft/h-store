package org.jaga.exampleApplications.proteinLocation;

import org.jaga.individualRepresentation.proteinLocation.*;
import org.jaga.masterAlgorithm.ElitistGA;
import org.jaga.definitions.*;
import org.jaga.fitnessEvaluation.proteinLocation.*;
import org.jaga.util.*;
import org.jaga.masterAlgorithm.*;
import org.jaga.reproduction.*;
import org.jaga.reproduction.proteinLocation.*;
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

public class SubcellularProteinLocation {

	public SubcellularProteinLocation() {}

	public void exec() {

		GAParameterSet params = new DefaultParameterSet();

		SimplifiedFastaFileParser parser = new SimplifiedFastaFileParser();
		ProteinGroup cytosol = new ProteinGroup("Cytosol", parser, "D:/Courseworks/4C58/cw/data/Cytosol.train.dat");
		ProteinGroup extracellular = new ProteinGroup("Extracellular", parser, "D:/Courseworks/4C58/cw/data/Extracellular.train.dat");
		ProteinGroup nucleus = new ProteinGroup("Nucleus", parser, "D:/Courseworks/4C58/cw/data/Nucleus.train.dat");
		ProteinGroup mitochondrion = new ProteinGroup("Mitochondrion", parser, "D:/Courseworks/4C58/cw/data/Mitochondrion.train.dat");

		ProteinGroup allPositives = new ProteinGroup();
		allPositives.add(nucleus);
		ProteinGroup allNegatives = new ProteinGroup();
		allNegatives.add(mitochondrion);
		allNegatives.add(extracellular);
		allNegatives.add(cytosol);
		ProteinGroup testPositives = new ProteinGroup();
		ProteinGroup testNegatives = new ProteinGroup();
		params.setFitnessEvaluationAlgorithm(new ProteinLocationTrainer(testPositives, testNegatives, 3));
		ProgressiveTestGroupSizeHook progressHook = new ProgressiveTestGroupSizeHook(
					allPositives, allNegatives, testPositives, testNegatives,
					30, 2, 0.41, 3., 200, 3, params);


		params.setPopulationSize(30);

		CombinedReproductionAlgorithm repAlg = new CombinedReproductionAlgorithm();
		repAlg.insertReproductionAlgorithm(0, new PolypeptidePatternXOver(1, 0.85));
		PolypeptidePatternMutation mutation = new PolypeptidePatternMutation(0.1);
		repAlg.insertReproductionAlgorithm(1, mutation);
		repAlg.insertReproductionAlgorithm(2, new PolypeptidePatternElongation(0.0005));
		params.setReproductionAlgorithm(repAlg);

		params.setMaxGenerationNumber(10000);

		params.setSelectionAlgorithm(//new TwoTournamentProbabalisticSelection(0.9));
					new TournamentSelection(5, 0.9));
					//new RouletteWheelSelection(-5));

		ProteinLocationClassifierFactory factory = new ProteinLocationClassifierFactory();
		params.setIndividualsFactory(factory);

		final int attempts = 1;

		//ReusableSimpleGA ga = new ReusableSimpleGA(params);
		ElitistGA ga = new ElitistGA(params, 0.1, 0.0);   // params, elite, bad
		AnalysisHook analysis = new AnalysisHook();
		analysis.setLogStream(System.out);
		analysis.setUpdateDelay(1500);
		ga.addHook(analysis);
		ga.addHook(progressHook);
		progressHook.setAnalysisHookForCooperation(analysis);
		GAResult [] allResults = new GAResult[attempts];

		for (int i = 0; i < attempts; i++) {

			System.out.println("\n ========== STARTING RUN " + (i+1) + ". ==============================\n");
			analysis.reset();

			GAResult result = new FittestIndividualResult();
			try {
				result = ((ReusableSimpleGA) ga).exec();
			} catch (OutOfMemoryError e) {
				e.printStackTrace();
			}

			System.out.println("\nDONE.\n");
			System.out.println("Total fitness evaluations: " + analysis.getFitnessCalculations());
			System.out.println("Result is: " + result);
			System.out.println("(Fitness: " + ((FittestIndividualResult) result).getBestFitness() + ")");
			allResults[i] = result;
		}

		System.out.println("\nALL DONE.\n");
		for (int i = 0; i < attempts; i++) {
			System.out.println("Result " + i + " is: " + allResults[i]);
			System.out.println("(Fitness " + i + " is:" + ((FittestIndividualResult) allResults[i]).getBestFitness() + ")");
		}

	}

	public static void main(String[] unusedArgs) {
		SubcellularProteinLocation subcellularProteinLocation = new SubcellularProteinLocation();
		subcellularProteinLocation.exec();
	}

}