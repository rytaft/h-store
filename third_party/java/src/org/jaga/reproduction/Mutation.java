package org.jaga.reproduction;

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

abstract public class Mutation implements ReproductionAlgorithm {

	private double mutationProbability = 0.05;

	public Mutation() {}

	public Mutation(double mutProb) {
		setMutationProbability(mutProb);
	}

	public int getRequiredNumberOfParents() {
		return -1;
	}

	public void setMutationProbability(double mutProb) {
		if (mutProb < 0.0)
			throw new IllegalArgumentException("MutationProbability may not be below 0 (is "
											   + mutProb + ")");
		if (mutProb > 1.0)
			throw new IllegalArgumentException("MutationProbability may not be above 1 (is "
											   + mutProb + ")");
		this.mutationProbability = mutProb;
	}

	public double getMutationProbability() {
		return this.mutationProbability;
	}

	abstract public Class getApplicableClass();

	abstract public Individual[] reproduce(Individual[] parents, GAParameterSet params);

}