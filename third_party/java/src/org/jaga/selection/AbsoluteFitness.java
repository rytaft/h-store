package org.jaga.selection;

import org.jaga.definitions.Fitness;

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

public class AbsoluteFitness implements Fitness {

	private double value = +0.0;

	private AbsoluteFitness() {}

	public AbsoluteFitness(double fitnessValue) {
		resetValue(fitnessValue);
	}

	public void resetValue(double fitnessValue) {
		if (Double.isNaN(fitnessValue))
			throw new IllegalArgumentException("Agrument is not a number");
		else if (fitnessValue == Double.NEGATIVE_INFINITY)
			this.value = -Double.MAX_VALUE;
		else if (fitnessValue == Double.POSITIVE_INFINITY)
			this.value = Double.MAX_VALUE;
		else
			this.value = fitnessValue;
	}

	public double getValue() {
		return this.value;
	}

	public boolean isBetter(Fitness fitness) {
		if (null == fitness)
			return true;
		AbsoluteFitness absFit = (AbsoluteFitness) fitness;
		return this.getValue() > absFit.getValue();
	}

	public boolean isWorse(Fitness fitness) {
		if (null == fitness)
			return false;
		AbsoluteFitness absFit = (AbsoluteFitness) fitness;
		return this.getValue() < absFit.getValue();
	}

	public String toString() {
		return Double.toString(value);
	}

}