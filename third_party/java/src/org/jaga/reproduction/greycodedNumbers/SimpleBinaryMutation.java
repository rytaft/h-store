package org.jaga.reproduction.greycodedNumbers;

import org.jaga.definitions.*;
import org.jaga.util.*;

import org.jaga.individualRepresentation.greycodedNumbers.*;
import org.jaga.reproduction.*;

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

public class SimpleBinaryMutation extends Mutation {

	private static final Class applicableClass = BinaryEncodedIndividual.class;

	public Class getApplicableClass() {
		return applicableClass;
	}

	public SimpleBinaryMutation() {
		super();
	}

	public SimpleBinaryMutation(double mutProb) {
		super(mutProb);
	}

	public Individual [] reproduce(Individual[] parents, GAParameterSet params) {
		final int kidsCount = parents.length;
		BinaryEncodedIndividual [] kids = new BinaryEncodedIndividual[kidsCount];
		final RandomGenerator rnd = params.getRandomGenerator();
		final double mutProb = getMutationProbability();
		final NDecimalsIndividualSimpleFactory factory =
				(NDecimalsIndividualSimpleFactory) params.getIndividualsFactory();

		for (int i = 0; i < kidsCount; i++) {

			final int maxAttempts = params.getMaxBadReproductionAttempts();
			int attempts = 0;
			boolean kidIsValid = false;
			do {

				if (!getApplicableClass().isInstance(parents[i]))
					fireIllegalParentException(parents, i);

				BitString kidBits = (BitString) ((BinaryEncodedIndividual) parents[i]).getBitStringRepresentation().clone();
				for (int b = 0; b < kidBits.getLength(); b++)
					if (rnd.nextDouble() < mutProb)
						kidBits.flip(b);

				NDecimalsIndividual tst = (NDecimalsIndividual)
								factory.createSpecificIndividual(kidBits, params);
				kidIsValid = factory.valid(tst);

				if (kidIsValid)
					kids[i] = tst;

				attempts++;
			} while(!kidIsValid && attempts <= maxAttempts);

			if (!kidIsValid) {
				kids[i] = (BinaryEncodedIndividual) factory.createSpecificIndividual(
											((BinaryEncodedIndividual)parents[i]).getBitStringRepresentation(),
											params);
				kids[i].setFitness(parents[i].getFitness());
			}

		}

		return kids;
	}

	private void fireIllegalParentException(Individual[] parents, int i)
														   throws IllegalArgumentException {
		throw new IllegalArgumentException("SimpleBinaryMutation works "
										   + "only on parents of type " + getApplicableClass()
										   +", but parent number " + i + " is of type "
										   + parents[i].getClass().getName());
	}

}