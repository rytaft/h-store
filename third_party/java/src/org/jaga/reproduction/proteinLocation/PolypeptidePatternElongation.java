package org.jaga.reproduction.proteinLocation;

import org.jaga.reproduction.Mutation;
import org.jaga.individualRepresentation.proteinLocation.*;
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

public class PolypeptidePatternElongation extends Mutation {

	public PolypeptidePatternElongation() {
		super();
	}

	public PolypeptidePatternElongation(double mutProb) {
		super(mutProb);
	}

	public Individual reproduce(Individual parent, GAParameterSet params) {
		ProteinLocationClassifier kid = (ProteinLocationClassifier)
								params.getIndividualsFactory().createSpecificIndividual(parent, params);
		PolypeptidePattern pattern = kid.getPattern();
		RandomGenerator rnd = params.getRandomGenerator();
		double mutProb = getMutationProbability();
		ProteinLocationClassifierFactory factory = (ProteinLocationClassifierFactory) params.getIndividualsFactory();
		for (int i = 0; i <= pattern.getLength(); i++)
			if (rnd.nextDouble() < mutProb)
				if (pattern.getLength() < factory.getMaxPatternLength())
					pattern.insertItem(i, factory.createRandomPatternItem(params));
		return kid;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {
		Individual [] kids = new Individual[parents.length];
		for (int i = 0; i < parents.length; i++)
			kids[i] = reproduce(parents[i], params);
		return kids;
	}

	public Class getApplicableClass() {
		return ProteinLocationClassifier.class;
	}
}