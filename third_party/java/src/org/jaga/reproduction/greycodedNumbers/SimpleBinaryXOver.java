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

public class SimpleBinaryXOver extends XOver {

	private static final Class applicableClass = BinaryEncodedIndividual.class;

	public SimpleBinaryXOver() {
		super();
	}

	public SimpleBinaryXOver(double xOverProb) {
		super(xOverProb);
	}

	public Class getApplicableClass() {
		return applicableClass;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {
		if (parents.length != getRequiredNumberOfParents())
			throw new IllegalArgumentException("Need " + getRequiredNumberOfParents()
											   + " parents for reproduction (not "
											   + parents.length + ")");

		// Check correct type for parents, get length:
		int bitLen = checkParentsTypeAndLength(parents);

		// Chance (1 - xover probability) that parents wont be changed:
		final RandomGenerator rnd = params.getRandomGenerator();

		if (rnd.nextDouble() >= getXOverProbability())
			return makeCopyOfParents(parents, params);

		// Get parents bitsrings:
		BitString p1 = ((BinaryEncodedIndividual) parents[0]).getBitStringRepresentation();
		BitString p2 = ((BinaryEncodedIndividual) parents[1]).getBitStringRepresentation();

		// x-over:
		final int maxAttempts = params.getMaxBadReproductionAttempts();

		int attempts = 0;
		boolean kidsAreValid = false;
		do {
			kidsAreValid = false;
			int xPoint = rnd.nextInt(1, bitLen);

			// offspring bit strings:
			BitString c1 = new BitString(bitLen);
			BitString c2 = new BitString(bitLen);

			// copy before xover-point:
			for (int i = 0; i < xPoint; i++) {
				c1.set(i, p1.get(i));
				c2.set(i, p2.get(i));
			}

			// copy after xover-point:
			for (int i = xPoint; i < bitLen; i++) {
				c1.set(i, p2.get(i));
				c2.set(i, p1.get(i));
			}

			// create children and check if children are valid:
			NDecimalsIndividual [] kids = createKidsFromEncoding(params, c1, c2);
			kidsAreValid = kidsSatisfyConstraints(kids, params);

			// return valid kids or have another attempts:
			if (kidsAreValid)
				return kids;
			else
				attempts++;

		} while(!kidsAreValid && attempts < maxAttempts);

		// all attempts failed:
		return makeCopyOfParents(parents, params);

	}

	protected Individual [] makeCopyOfParents(Individual [] parents, GAParameterSet params) {
		Individual [] kids = new Individual[parents.length];
		for (int i = 0; i < kids.length; i++) {
			kids[i] = params.getIndividualsFactory().createSpecificIndividual(
						   ((BinaryEncodedIndividual)parents[i]).getBitStringRepresentation(),
						   params);
			//kids[i] = (BinaryEncodedIndividual) ((BinaryEncodedIndividual) parents[i]).clone();
			kids[i].setFitness(parents[i].getFitness());
		}
		return kids;
	}

	private int checkParentsTypeAndLength(Individual[] parents) throws IllegalArgumentException {
		// Now make sure that parents are of right type and equal length:
			  int bitLen = -1;
			  for (int i = 0; i < parents.length; i++) {
				  // Check type:
				  if (!getApplicableClass().isInstance(parents[i]))
					  throw new IllegalArgumentException("SimpleBinaryXOver works only for "
													   + getApplicableClass().getName()
													   + ", but parent is "
													   + parents[i].getClass().getName());
				  if (0 == i) // Remember bit len of first parent:
					  bitLen = ((BinaryEncodedIndividual) parents[i]).getBitStringRepresentation().getLength();
				  else        // And compare it to the bit len of all other parents:
					  if (bitLen != ((BinaryEncodedIndividual) parents[i]).getBitStringRepresentation().getLength())
						  throw new IllegalArgumentException("SimpleBinaryXOver works only on "
												  + getApplicableClass() + " of equal representation length ("
												  + bitLen  + "!="
												  + ((BinaryEncodedIndividual) parents[i]).getBitStringRepresentation().getLength());
			  }
		return bitLen;
	}

	private boolean kidsSatisfyConstraints(NDecimalsIndividual[] kids, GAParameterSet params) {
		NDecimalsIndividualSimpleFactory fact = (NDecimalsIndividualSimpleFactory) params.getIndividualsFactory();
		for (int i = 0; i < kids.length; i++)
			if (!fact.valid(kids[i]))
				return false;
		return true;
	}

	private NDecimalsIndividual [] createKidsFromEncoding(GAParameterSet params, BitString c1, BitString c2) {
		NDecimalsIndividual [] kids = new NDecimalsIndividual[getRequiredNumberOfParents()];
		kids[0] = (NDecimalsIndividual)
					 params.getIndividualsFactory().createSpecificIndividual(c1, params);
		kids[1] = (NDecimalsIndividual)
					 params.getIndividualsFactory().createSpecificIndividual(c2, params);
		return kids;
	}

}