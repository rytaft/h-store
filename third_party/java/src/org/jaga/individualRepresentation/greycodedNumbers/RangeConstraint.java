/*
 * Created on 13.04.2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package org.jaga.individualRepresentation.greycodedNumbers;

/**
 * Represents a constraint to the range of values a decimal variable inside an
 * {@link NDecimalsIndividual} can assume.
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
public class RangeConstraint {

	double min = -Double.MAX_VALUE;
	double max = Double.MAX_VALUE;

	public RangeConstraint() {}

	public RangeConstraint(double min, double max) {
		setRange(min, max);
	}

	private void setRange(double min, double max) {
		if (min > max)
			throw new IllegalArgumentException("Invalid (empty) range ["
										   + min + ", " + max + "]");
		this.min = min;
		this.max = max;
	}

	public double getMinValue() {
		return this.min;
	}

	public double getMaxValue() {
		return this.max;
	}

}
