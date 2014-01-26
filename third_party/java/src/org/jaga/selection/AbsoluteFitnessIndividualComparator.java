package org.jaga.selection;


import java.util.Comparator;
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

public class AbsoluteFitnessIndividualComparator implements Comparator {

	public AbsoluteFitnessIndividualComparator() {
	}

	public int compare(Object o1, Object o2) {
		Individual ind1 = (Individual) o1;
		Individual ind2 = (Individual) o2;
		AbsoluteFitness fit1 = (AbsoluteFitness) ind1.getFitness();
		AbsoluteFitness fit2 = (AbsoluteFitness) ind2.getFitness();

		double diff = fit1.getValue() - fit2.getValue();

		if (-1 < diff && diff < 0)
			diff = -1;
		if (0 < diff && diff < 1)
			diff = 1;
		return (int) diff;
	}

	public boolean equals(Object obj) {
		return this.equals(obj);
	}

}