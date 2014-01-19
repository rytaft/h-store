package org.jaga.selection;

import org.jaga.definitions.*;


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

public class RouletteWheelSelection implements SelectionAlgorithm {

	private static final Class applicableFitnessClass = AbsoluteFitness.class;
	public static final double MIN_FITNESS_LIMIT = -Double.MAX_VALUE;

	private double minFitness = MIN_FITNESS_LIMIT;

	public RouletteWheelSelection() {}

	public RouletteWheelSelection(double minFitness) {
		setMinFitness(minFitness);
	}

	public double getMinFitness() {
		return this.minFitness;
	}

	public void setMinFitness(double minFitness) {
		if (Double.isNaN(minFitness)
				|| Double.NEGATIVE_INFINITY == minFitness
				|| Double.POSITIVE_INFINITY == minFitness)
			throw new IllegalArgumentException("Minumum fitness is an illegal "
											   + "value or larger then maximum "
											   + " fitness (" + minFitness + ")");
		this.minFitness = minFitness;
	}

	public Class getApplicableFitnessClass() {
		return applicableFitnessClass;
	}

	public Individual select(Population population, int age, GAParameterSet params) {
		double cumSum = calculateCumulativeFitness(population);
		return spinRoulette(cumSum, population, params);
	}

	public Individual [] select(Population population, int howMany, int age, GAParameterSet params) {
		double cumSum = calculateCumulativeFitness(population);
		Individual [] selection = new Individual[howMany];
		for (int i = 0; i < howMany; i++) {
			selection[i] = spinRoulette(cumSum, population, params);
		}
		return selection;
	}

	private double calculateCumulativeFitness(Population population) {
		double sum = 0;
		for (int i = 0; i < population.getSize(); i++) {
			double fitVal = getRelativeFitnessValue(population.getMember(i));
			sum += fitVal;
		}
		if (sum == Double.POSITIVE_INFINITY || Double.isNaN(sum))
			throw new IllegalStateException("\nCumulative fitness is " + sum
											+ ", which is over the bound. "
											+ "Maybe lower fitness bound is "
											+ "set too low? (" + this.minFitness + ")");
		return sum;
	}

	private Individual spinRoulette(double cumSum, Population pop, GAParameterSet params) {

		int popSize = pop.getSize();

		if (0 == cumSum)
			return pop.getMember(params.getRandomGenerator().nextInt(0, popSize));

		double roulette = params.getRandomGenerator().nextDouble(0, cumSum);

		double loB = 0.0;
		double upB = 0.0;
		for (int i = 0; i < popSize; i++) {
			Individual indv = pop.getMember(i);
			double fval = getRelativeFitnessValue(indv);
			upB += fval;
			if (loB <= roulette && roulette < upB)
				return indv;
			loB = upB;
		}

		throw new Error("Something is dodgy, this line should never be executed!");
	}

	private double getRelativeFitnessValue(Individual indv) {
		Fitness f = indv.getFitness();
		assert null != f : "Individuall has null fitness";
		AbsoluteFitness fit = (AbsoluteFitness) f;
		double rval = fit.getValue() - this.minFitness;
		if (0 > rval)
			throw new IllegalArgumentException("\nIndividual has a fitness value smaller "
							+ "then the boundary (" + fit.getValue() + " < " + this.minFitness);
		return rval;
	}

}