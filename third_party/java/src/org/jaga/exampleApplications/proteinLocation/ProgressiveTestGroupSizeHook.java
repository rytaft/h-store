package org.jaga.exampleApplications.proteinLocation;

import org.jaga.selection.AbsoluteFitness;
import org.jaga.individualRepresentation.proteinLocation.*;
import org.jaga.definitions.*;
import org.jaga.util.*;
import org.jaga.masterAlgorithm.*;
import org.jaga.reproduction.*;
import org.jaga.reproduction.proteinLocation.*;
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

public class ProgressiveTestGroupSizeHook extends SimpleGAHook {

	private double mutationBoost = 3.0;
	private int boostStartTimeout = 75;
	private int boostStopTimeout = 5;

	private ProteinGroup allPositives = null;
	private ProteinGroup allNegatives = null;
	private ProteinGroup testPositives = null;
	private ProteinGroup testNegatives = null;
	private int testSetSizeDelta = 0;
	private double triggerFitness = 0;
	private int allPositivesIndex = 0;
	private int allNegativesIndex = 0;

	private AnalysisHook cooperator = null;

	private double averagePatternLen = 0;
	private int averagePatternLenGen = 0;
	private double mutProb = 0;
	private int lastSizeIncreaseGen = 0;
	private boolean boosting = false;
	private int timeToBoost = boostStartTimeout;
	private int timeToStopBoost = 0;

	private ProgressiveTestGroupSizeHook() {}

	public ProgressiveTestGroupSizeHook(ProteinGroup allPositives, ProteinGroup allNegatives,
										ProteinGroup testPositives, ProteinGroup testNegatives,
										int initialTestSetSize, int testSetSizeDelta,
										double triggerFitness, double mutationBoost,
										int boostStartTimeout, int boostStopTimeout,
										GAParameterSet params) {
		this.mutationBoost = mutationBoost;
		this.boostStartTimeout = boostStartTimeout;
		this.boostStopTimeout = boostStopTimeout;
		this.timeToBoost = boostStartTimeout;
		this.allPositives = allPositives;
		this.allNegatives = allNegatives;
		this.testPositives = testPositives;
		this.testNegatives = testNegatives;
		this.testSetSizeDelta = testSetSizeDelta;
		this.triggerFitness = triggerFitness;
		this.allPositivesIndex = 0;
		this.allNegativesIndex = 0;
		allPositives.flatten();
		allPositives.randomise(params.getRandomGenerator());
		allNegatives.flatten();
		allNegatives.randomise(params.getRandomGenerator());
		while (allPositivesIndex < allPositives.size()
			   && testPositives.size() < initialTestSetSize) {
			testPositives.add(allPositives.getProtein(allPositivesIndex));
			allPositivesIndex++;
		}
		while (allNegativesIndex < allNegatives.size()
			   && testNegatives.size() < initialTestSetSize) {
			testNegatives.add(allNegatives.getProtein(allNegativesIndex));
			allNegativesIndex++;
		}
		printInfo();
	}

	private void startBoost(GAParameterSet params) {

		ReproductionAlgorithm a = params.getReproductionAlgorithm();
		if (!(a instanceof CombinedReproductionAlgorithm))
			return;
		CombinedReproductionAlgorithm algs = (CombinedReproductionAlgorithm) a;

		for (int i = 0; i < algs.countCombinedAlgorithms(); i++) {
			a = algs.getReproductionAlgorithm(i);
			if (a instanceof PolypeptidePatternMutation) {
				mutProb = ((PolypeptidePatternMutation) a).getMutationProbability();
				double newProb = mutProb * mutationBoost;
				if (newProb > 1)
					newProb = 1;
				((PolypeptidePatternMutation) a).setMutationProbability(newProb);
			}
		}

		boosting = true;
		timeToStopBoost = boostStopTimeout;

	}

	private void stopBoost(GAParameterSet params) {
		ReproductionAlgorithm a = params.getReproductionAlgorithm();
		if (!(a instanceof CombinedReproductionAlgorithm))
			return;
		CombinedReproductionAlgorithm algs = (CombinedReproductionAlgorithm) a;

		for (int i = 0; i < algs.countCombinedAlgorithms(); i++) {
			a = algs.getReproductionAlgorithm(i);
			if (a instanceof PolypeptidePatternMutation) {
				((PolypeptidePatternMutation) a).setMutationProbability(mutProb);
			}
		}

		boosting = false;
		timeToBoost = boostStartTimeout;
	}

	public void foundNewResult(SimpleGA caller, Population pop, int age,
							   GAResult result, GAParameterSet params) {
		if (boosting)
			stopBoost(params);
		this.timeToBoost = this.boostStartTimeout;
		averagePatternLen = 0;
		for (int i = 0; i < pop.getSize(); i++) {
			final Individual ind = pop.getMember(i);
			averagePatternLen += ((ProteinLocationClassifier) ind).getPattern().getLength();
		}
		averagePatternLen /= (double) pop.getSize();
		averagePatternLenGen = age;
	}

	public void generationChanged(SimpleGA caller, Population pop, int age,
								  GAResult result, GAParameterSet params) {

		printInfo();

		// Get currently best fitness:

		if (null == result)
			return;

		Individual bestInd = ((FittestIndividualResult) result).
							 getFittestIndividual();
		if (null == bestInd)
			return;

		AbsoluteFitness bestFit = (AbsoluteFitness) bestInd.getFitness();
		if (null == bestFit)
			return;

		// Check if fitness reched the trigger level:

		if (boosting) {
			if (timeToStopBoost == 0)
				stopBoost(params);
			else
				timeToStopBoost--;
		} else { // if (!boosting)
			if (timeToBoost == 0)
				startBoost(params);
			else
				timeToBoost--;

		}

		if (bestFit.getValue() < triggerFitness)
			return;

		if (boosting) // if still boosting but noe better fitness, stop boosting
			stopBoost(params);

		if (allPositives.size() == testPositives.size()
				&& allNegatives.size() == testNegatives.size()) {
			return;
		}

		// Enlarge test sets:

		int targetSize = testPositives.size() + testSetSizeDelta;
		while (allPositivesIndex < allPositives.size()
			   && testPositives.size() < targetSize) {
			testPositives.add(allPositives.getProtein(allPositivesIndex));
			allPositivesIndex++;
		}

		targetSize = testNegatives.size() + testSetSizeDelta;
		while (allNegativesIndex < allNegatives.size()
			   && testNegatives.size() < targetSize) {
			testNegatives.add(allNegatives.getProtein(allNegativesIndex));
			allNegativesIndex++;
		}

		// Recalculate fitnesses:

		lastSizeIncreaseGen = age;
		timeToBoost = boostStartTimeout;
		averagePatternLen = 0;
		int bestI = -1;
		double bestF = Double.NEGATIVE_INFINITY;
		FitnessEvaluationAlgorithm tester = params.getFitnessEvaluationAlgorithm();
		for (int i = 0; i < pop.getSize(); i++) {
			final Individual ind = pop.getMember(i);
			averagePatternLen += ((ProteinLocationClassifier) ind).getPattern().getLength();
			final Fitness newIndFit = tester.evaluateFitness(ind, age, pop,
				  params);
			final double fVal = ((AbsoluteFitness) newIndFit).getValue();
			ind.setFitness(newIndFit);
			if (fVal > bestF) {
				bestI = i;
				bestF = fVal;
			}
		}
		((FittestIndividualResult) result).setFittestIndividual(pop.getMember(bestI));
		averagePatternLen /= (double) pop.getSize();

		if (null != cooperator) {
			cooperator.populationReinitialised(pop, age, result, params);
		}
	}

	public void setAnalysisHookForCooperation(AnalysisHook cooperator) {
		this.cooperator = cooperator;
	}

	private void printInfo() {
		System.out.println();
		System.out.println(" *** **  Active positive test set size: " + testPositives.size()
						   + " ( can grow up to " + allPositives.size() + ")");
		System.out.println(" *** **  Active negative test set size: " + testNegatives.size()
						   + " ( can grow up to " + allNegatives.size() + ")");
		System.out.println(" *** **  Test set last increased in generation " + lastSizeIncreaseGen);
		System.out.println("     **  Avegare length of classifier in generation "
						   + averagePatternLenGen + " was: " + averagePatternLen);
		if (boosting)
			System.out.println(" *** **  BOOSTING mustation probability. Time left: "
							   + timeToStopBoost);
		else
			System.out.println(" *** **  Time till mutation boost: " + timeToBoost);
	}

}