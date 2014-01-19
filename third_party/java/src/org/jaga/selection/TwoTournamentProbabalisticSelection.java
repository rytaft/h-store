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

public class TwoTournamentProbabalisticSelection implements SelectionAlgorithm {

	private double betterCandidateProbability = 0.7;
	private static final Class applicableFitnessClass = AbsoluteFitness.class;

	public TwoTournamentProbabalisticSelection() {
	}

	public TwoTournamentProbabalisticSelection(double betterCandProb) {
		setBetterCandidateProbability(betterCandProb);
	}

	public double getBetterCandidateProbability() {
		return this.betterCandidateProbability;
	}

	public void setBetterCandidateProbability(double betterCandProb) {
		if (betterCandProb < 0 || 1 < betterCandProb)
			throw new IllegalArgumentException("Better candidate selection "
											   + "probability must be in [0, 1]");
		this.betterCandidateProbability = betterCandProb;
	}

	public Individual select(Population population, int age, GAParameterSet params) {
		RandomGenerator rnd = params.getRandomGenerator();

		int popSize = population.getSize();

		int p = rnd.nextInt(0, popSize);
		Individual comp1 = population.getMember(p);

		p = rnd.nextInt(0, popSize);
		Individual comp2 = population.getMember(p);

		AbsoluteFitness fit1 = (AbsoluteFitness) comp1.getFitness();
		AbsoluteFitness fit2 = (AbsoluteFitness) comp2.getFitness();
		if (fit2.isBetter(fit1)) {
			Individual t = comp1;
			comp1 = comp2;
			comp2 = t;
		}

		double dice = rnd.nextDouble();
		if (dice < betterCandidateProbability)
			return comp1;
		else
			return comp2;
	}

	public Individual[] select(Population population, int howMany, int age, GAParameterSet params) {
		Individual [] selection = new Individual[howMany];
		for (int i = 0; i < howMany; i++) {
			selection[i] = select(population, age, params);
		}
		return selection;
	}

	public Class getApplicableFitnessClass() {
		return applicableFitnessClass;
	}

}
