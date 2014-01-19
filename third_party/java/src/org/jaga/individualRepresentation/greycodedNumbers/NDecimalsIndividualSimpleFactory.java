package org.jaga.individualRepresentation.greycodedNumbers;

import org.jaga.definitions.*;
import org.jaga.util.*;

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

public class NDecimalsIndividualSimpleFactory implements IndividualsFactory {

	private int individualSize = 1;
	private double decimalScale = 1000000;  // 10^6 i.e., 6 digits
	private int precision = 64;  // bits
	private RangeConstraint [] constraints = new RangeConstraint [] {null};

	public NDecimalsIndividualSimpleFactory() {}

	public NDecimalsIndividualSimpleFactory(int varsPerIndividual, int decPrecision,
											int representationLen) {
		setIndividualSize(varsPerIndividual);
		setDecimalScale(decPrecision);
		setPrecision(representationLen);
	}

	public int getDecimalScale() {
		double scal = Math.log(decimalScale) / Math.log(10); // i.e. log(10, scale)
		return (byte) scal;
	}

	public void setDecimalScale(int val) {
		if (val < 0 || 20 < val)
			throw new IllegalArgumentException("Decimal scale (" + val
											   + ") not supported, use 10^0 - 10^20");
		decimalScale = Math.pow(10, val);
	}

	public int getIndividualSize() {
		return individualSize;
	}

	public void setIndividualSize(int size) {
		RangeConstraint [] newConstraints = new RangeConstraint[size];

		for (int i = 0; i < Math.min(size, this.individualSize); i++)
			newConstraints[i] = this.constraints[i];

		for (int i = Math.min(size, this.individualSize); i < size; i++)
			newConstraints[i] = null;

		this.individualSize = size;
		this.constraints = newConstraints;
	}

	/**
	 * Gets the constraint for individuals' variable with specified index.
	 *
	 * @param variableIndex the index of the variable inside the individuals to
	 * which the constraint applies.
	 *
	 * @return the constraint vor variables at the specified index.
	 */
	public RangeConstraint getConstraint(int variableIndex) {
		return constraints[variableIndex];
	}

	/**
	 * Sets the constraints for individuals' variable at the specified index.
	 *
	 * @param variableIndex index of the variable to which the consteraint applies.
	 * @param constraint the constraint.
	 */
	public void setConstraint(int variableIndex, RangeConstraint constraint) {
		constraints[variableIndex] = constraint;
	}

	public int getPrecision() {
		return precision;
	}

	public void setPrecision(int val) {
		precision = val;
	}

	/**
	 * Checks if the values of the specified individual are in the ranges
	 * specified by constraints of this factory.
	 *
	 * @param indiv some individual
	 *
	 * @return <code>true</code> if all values encoded by this individual are inside the
	 * ranges specified by the constrains applicable to this factory;
	 * <code>false</code> otherwise.
	 */
	public boolean valid(NDecimalsIndividual indiv) {

		// check if individual's settings match this factiory:
		if (indiv.getDecimalScale() != this.getDecimalScale()
				|| indiv.getSize() != this.getIndividualSize()
				|| indiv.getPrecision() != this.getPrecision()) {
			throw new IllegalArgumentException("The given individual (" + indiv
											 + ") is likely not created by this factory.");
		}

		// check all values of the individual:
		for (int i = 0; i < indiv.getSize(); i++) {
			double minmax[] = getAllowedRange(i);
			double val = indiv.getDoubleValue(i);
			if (val < minmax[0])
				return false;
			if (val > minmax[1])
				return false;
		}
		return true;
	}

	public Individual createDefaultIndividual(GAParameterSet params) {
		// Create a default individual:
		NDecimalsIndividual indiv = new NDecimalsIndividual(individualSize, decimalScale, precision);

		// If some value is out of range, set it in the middle of range:
		for (int valInd = 0; valInd < individualSize; valInd++) {
			double [] minmax = getAllowedRange(valInd);
			if (indiv.getDoubleValue(valInd) < minmax[0] || minmax[1] < indiv.getDoubleValue(valInd)) {
				double val = 0.5 * minmax[0] + 0.5 * minmax[1];
				indiv.setDoubleValue(valInd, val);
			}
		}

		return indiv;
	}

	public Individual createRandomIndividual(GAParameterSet params) {
		// create an individual:
		NDecimalsIndividual indiv = new NDecimalsIndividual(individualSize, decimalScale, precision);

		// set all values to uniformly distributed random values:
		for (int valInd = 0; valInd < individualSize; valInd++) {
			double [] minmax = getAllowedRange(valInd);
			double val = params.getRandomGenerator().nextDouble(minmax[0], minmax[1]);
			indiv.setDoubleValue(valInd, val);
		}
		return indiv;
	}

	public Individual createSpecificIndividual(Object init, GAParameterSet params) {

		if (null == init)
			throw new NullPointerException("Initialisation value for NDecimalsIndividual my not be null");

		if (init instanceof NDecimalsIndividual)
			return createSpecificIndividual((NDecimalsIndividual) init);

		if (init instanceof BitString)
					return createSpecificIndividual((BitString) init);

		if (init instanceof BitString)
					return createSpecificIndividual((BitString) init);

		if (init instanceof double [])
			return createSpecificIndividual((double []) init);

		throw new ClassCastException("Initialisation value for NDecimalsIndividual "
									 + "must be of type BitString or Double (but is "
									 + init.getClass() + ")");
	}

	public Individual createSpecificIndividual(NDecimalsIndividual initVal) {
		NDecimalsIndividual indiv = new NDecimalsIndividual(individualSize, decimalScale, precision);
		indiv.setBitStringRepresentation(initVal.getBitStringRepresentation());
		return indiv;
	}

	public Individual createSpecificIndividual(BitString initVal) {
		NDecimalsIndividual indiv = new NDecimalsIndividual(individualSize, decimalScale, precision);
		indiv.setBitStringRepresentation(initVal);
		return indiv;
	}

	public Individual createSpecificIndividual(double [] initVal) {
		NDecimalsIndividual indiv = new NDecimalsIndividual(individualSize, decimalScale, precision);
		for (int i = 0; i < individualSize; i++) {
			indiv.setDoubleValue(i, initVal[i]);
		}
		return indiv;
	}

	private double [] getAllowedRange(int varInd) throws IllegalArgumentException {

		// use these if no constraint applies:
		final double veryLargePositive = 0.99 * (Long.MAX_VALUE / decimalScale);
		final double veryLargeNegative = 0.99 * (Long.MIN_VALUE / decimalScale);

		// get constraint:
		RangeConstraint constr = constraints[varInd];

		// return range:
		if (null == constr)
			return new double[] {veryLargeNegative, veryLargePositive};

		return new double [] {Math.max(constr.getMinValue(), veryLargeNegative),
							  Math.min(constr.getMaxValue(), veryLargePositive)};
	}

}