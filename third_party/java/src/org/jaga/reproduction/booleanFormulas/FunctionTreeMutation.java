package org.jaga.reproduction.booleanFormulas;

import org.jaga.reproduction.booleanFormulas.nodes.*;
import org.jaga.individualRepresentation.booleanFormulas.*;
import org.jaga.reproduction.Mutation;
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

public class FunctionTreeMutation extends Mutation {

	private static final Class applicableClass = BooleanFormulaTree.class;

	public FunctionTreeMutation() {
		super();
	}

	public FunctionTreeMutation(double mutProb) {
		super(mutProb);
	}

	public Class getApplicableClass() {
		return applicableClass;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {

		Individual [] kids = new Individual[parents.length];
		for (int i = 0; i < kids.length; i++) {
			if (!getApplicableClass().isInstance(parents[i]))
				throw new ClassCastException("Incompatible parent class: must be "
											 + getApplicableClass().getName()
											 + ", but is " + parents.getClass().getName());
			kids[i] = mutate((BooleanFormulaTree) parents[i], params);
		}
		return kids;
	}

	private BooleanFormulaTree mutate(BooleanFormulaTree parent, GAParameterSet params) {

		BooleanFormulaTree kid = createSpecificIndividual(parent, params);

		if (params.getRandomGenerator().nextDouble() < getMutationProbability()) {
			final BooleanFormulaTreeFactory fact = fetchFactory(params);
			final Long handle = kid.selectRandomNode(params);
			int depth = kid.getNodeDepth(handle);
			BooleanFormulaTreeNode replacement = fact.createRandomNode(depth, params);
			kid.replaceNode(handle, replacement);
			kid.setFitness(null);
		}

		return kid;
	}

	private BooleanFormulaTree createSpecificIndividual(Object init, GAParameterSet params) {
		final BooleanFormulaTreeFactory fact = fetchFactory(params);
		final Individual form = (BooleanFormulaTree) fact.createSpecificIndividual(init, params);
		return (BooleanFormulaTree) form;
	}

	private BooleanFormulaTreeFactory fetchFactory(GAParameterSet params) {
		return (BooleanFormulaTreeFactory) params.getIndividualsFactory();
	}

}