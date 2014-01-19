package org.jaga.reproduction.booleanFormulas;

import org.jaga.reproduction.XOver;
import org.jaga.individualRepresentation.booleanFormulas.*;
import org.jaga.reproduction.booleanFormulas.nodes.BooleanFormulaTreeNode;
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

public class FunctionTreeXOver extends XOver {

	private static final Class applicableClass = BooleanFormulaTree.class;

	public FunctionTreeXOver() {
		super();
	}

	public FunctionTreeXOver(double xOverProb) {
		super(xOverProb);
	}

	public Class getApplicableClass() {
		return applicableClass;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {

		if (null == parents || parents.length != getRequiredNumberOfParents())
			throw new IllegalArgumentException("Must have " + getRequiredNumberOfParents()
											   + " parents");

		RandomGenerator rand = params.getRandomGenerator();
		Individual [] kids = copyParents(parents, params);

		if (getXOverProbability() > rand.nextDouble())
			return kids;

		BooleanFormulaTree mum = (BooleanFormulaTree) kids[0];
		BooleanFormulaTree dad = (BooleanFormulaTree) kids[1];

		int maxDepth = ((BooleanFormulaTreeFactory) params.getIndividualsFactory()).getMaxTreeDepth();
		int attempts = 0;
		boolean kidsAreValid = false;
		while (!kidsAreValid && attempts <= params.getMaxBadReproductionAttempts()) {

			Long mumHandle = mum.selectRandomNode(params);
			Long dadHandle = dad.selectRandomNode(params);

			int mumHeight = mum.getNodeHeight(mumHandle);
			int dadHeight = dad.getNodeHeight(dadHandle);
			int mumDepth = mum.getNodeDepth(mumHandle);
			int dadDepth = dad.getNodeDepth(dadHandle);

			if (mumDepth + dadHeight <= maxDepth || dadDepth + mumHeight <= maxDepth) {

				BooleanFormulaTreeNode mumNode = mum.exportNode(mumHandle);
				BooleanFormulaTreeNode dadNode = dad.exportNode(dadHandle);

				mum.replaceNode(mumHandle, dadNode);
				dad.replaceNode(dadHandle, mumNode);

				kidsAreValid = true;
				mum.setFitness(null);
				dad.setFitness(null);

			} else { // i.e. if mum + dad > maxDepth
				++attempts;
			}

		}

		return kids;
	}

	private Individual [] copyParents(Individual[] parents, GAParameterSet params) {
		BooleanFormulaTreeFactory factory = (BooleanFormulaTreeFactory) params.getIndividualsFactory();
		Individual [] clones = new Individual[parents.length];
		for (int i = 0; i < clones.length; i++) {
			clones[i] = factory.createSpecificIndividual(parents[i], params);
		}
		return clones;
	}

}