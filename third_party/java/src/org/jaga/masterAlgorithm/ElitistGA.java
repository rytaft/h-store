package org.jaga.masterAlgorithm;


import org.jaga.util.FittestIndividualResult;
import org.jaga.selection.*;
import org.jaga.definitions.*;
import java.util.Arrays;


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

public class ElitistGA extends ReusableSimpleGA {

	private double eliteProportion = 0.2;//propotion
	private double badProportion = 0.05;

	public ElitistGA() {}

	public ElitistGA(double eliteProportion) {
		setEliteProportion(eliteProportion);
	}

	public ElitistGA(double eliteProportion, double badProportion) {
		setEliteProportion(eliteProportion);
		setBadProportion(badProportion);
	}


	public ElitistGA(GAParameterSet parameters, double eliteProportion) {
		super(parameters);
		setEliteProportion(eliteProportion);
	}

	public ElitistGA(GAParameterSet parameters, double eliteProportion, double badProportion) {
		super(parameters);
		setEliteProportion(eliteProportion);
		setBadProportion(badProportion);
	}


	public double getEliteProportion() {
		return this.eliteProportion;
	}

	public void setEliteProportion(double eliteProportion) {
		if (eliteProportion < 0 || 1 < eliteProportion)
			throw new IllegalArgumentException("Elite proportion must be in [0, 1]");
		this.eliteProportion = eliteProportion;
	}

	public double getBadProportion() {
		return this.badProportion;
	}

	public void setBadProportion(double badProportion) {
		if (badProportion < 0 || 1 < badProportion)
			throw new IllegalArgumentException("Bad proportion must be in [0, 1]");
		this.badProportion = badProportion;
	}

	protected Population generateNextPopulation(Population oldPop, int age,
												GAResult result, GAParameterSet params) {

		FittestIndividualResult res = (FittestIndividualResult) result;
		Population newPop = createEmptyPopulation(params);
		final IndividualsFactory fact = params.getIndividualsFactory();

		// Cut bad:

		Individual [] pop = oldPop.getAllMembers();
		Arrays.sort(pop, new AbsoluteFitnessIndividualComparator());
		int cutSize = (int) ((double) pop.length * (1.0 - badProportion));
		Individual [] cutPop = new Individual[cutSize];
		System.arraycopy(pop, pop.length - cutSize, cutPop, 0, cutSize);
		  // the population we want is cutPop
		// Copy elite:

		int eliteSize = (int) ((double) params.getPopulationSize() * getEliteProportion());
		int p = cutSize - 1;
		while (newPop.getSize() < eliteSize) {
			Individual kid = fact.createSpecificIndividual(cutPop[p], params);
			kid.setFitness(cutPop[p].getFitness());
			newPop.add(kid);
			if (--p < 0)
				p = cutSize - 1;
		}

		// Copy rest:

		while (newPop.getSize() < params.getPopulationSize()) {

			Individual [] parents = selectForReproduction(oldPop, age, params);
			notifySelectedForReproduction(parents, oldPop, age, result, params);

			Individual [] children = haveSex(parents, params);
			for (int i = 0; i < children.length; i++) {
				if (null != children[i].getFitness())
					continue;
				updateIndividualFitness(children[i], oldPop, age, params);
				if (children[i].getFitness().isBetter(res.getBestFitness()))
					res.setFittestIndividual(children[i]);
			}

			notifyReproduced(children, parents, oldPop, age, result, params);
			newPop.addAll(children);
		}

		return newPop;
	}
}
