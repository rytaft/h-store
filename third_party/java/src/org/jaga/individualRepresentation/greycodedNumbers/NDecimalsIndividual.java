package org.jaga.individualRepresentation.greycodedNumbers;

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

public class NDecimalsIndividual extends NNumbersGreycodedIndivudual {

	private double decimalScale = 1000000.0; // 10^6 i.e., 6 digits

	public NDecimalsIndividual(int size, double decimalScale, int precision) {
		super(size, precision);
		this.decimalScale = decimalScale;
	}

	public double getDecimalScale() {
		return decimalScale;
	}

	public double getDoubleValue(int valueIndex) {
		BitString clear = getClearBitCode(valueIndex);
		double f = 1.0;
		double val = 0.0;
		for (int i = getPrecision() - 1; i >= 1; i--, f *= 2.0)
			val += (clear.get(i) ? 1.0 : 0.0) * f;
		if (clear.get(0))
			val = -val;
		val /= decimalScale;
		return val;
	}

	public void setDoubleValue(int valueIndex, double value) {
		double setVal = Math.rint(value * decimalScale);
		if (setVal >= Long.MAX_VALUE || setVal <= Long.MIN_VALUE)
			throw new IllegalArgumentException("Absolute decimal value too big");
		BitString clear = new BitString(getPrecision());
		clear.set(0, setVal < 0);
		setVal = Math.abs(setVal);
		double f = 1.0;
		for (int i = getPrecision() - 1; i >= 1; i--, f *= 2.0)
			clear.set(i, (setVal % (2.0 * f) >= f));
		setClearBitCode(valueIndex, clear);
	}

	public String toString() {
		final int size = getSize();
		StringBuffer s = new StringBuffer("{size=");
		s.append(size);
		s.append("; repr=");
		s.append(getBitStringRepresentation().toString());
		s.append("; vals=(");
		for (int i = 0; i < size; i++) {
			s.append(getDoubleValue(i));
			if (i < size - 1)
				s.append(", ");
			else
				s.append(")");
		}
		if (null == getFitness()) {
			s.append("; fitness-unknown}");
		} else {
			s.append("; fit=");
			s.append(getFitness().toString());
			s.append("}");
		}
		return s.toString();
	}

}