package org.jaga.selection;


import java.util.Arrays;
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

public class TournamentSelection implements SelectionAlgorithm {

	private static final Class applicableFitnessClass = AbsoluteFitness.class;

	private int competitorsNum = 2;
	private double betterCandidateProbability = 0.7;

	public TournamentSelection() {
	}

	public TournamentSelection(int oneOutOf, double betterCandProb) {
		setCompetitors(oneOutOf);
	}

	public int getCompetitiors() {
		return this.competitorsNum;
	}

	public void setCompetitors(int oneOutOf) {
		if (oneOutOf < 1)
			throw new IllegalArgumentException("Tournament must include at least 1 competitor");
		this.competitorsNum = oneOutOf;
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

	public Class getApplicableFitnessClass() {
		return applicableFitnessClass;
	}

	public Individual select(Population population, int age, GAParameterSet params) {
		RandomGenerator rnd = params.getRandomGenerator();
		int popSize = population.getSize();
		Individual [] competitors = new Individual[competitorsNum];
		for (int i = 0; i < competitorsNum; i++) {
			int p = rnd.nextInt(0, popSize);
			competitors[i] = population.getMember(p);
		}
		Arrays.sort(competitors, new AbsoluteFitnessIndividualComparator());

		int ci = competitorsNum - 1;
		for (; ; ) {
			double dice = rnd.nextDouble();
			if (dice < betterCandidateProbability)
				return competitors[ci];
			if(--ci < 0)
				ci = competitorsNum - 1;
		}
	}

	public Individual[] select(Population population, int howMany, int age, GAParameterSet params) {
		Individual [] selection = new Individual[howMany];
		for (int i = 0; i < howMany; i++) {
			selection[i] = select(population, age, params);
		}
		return selection;
	}

}