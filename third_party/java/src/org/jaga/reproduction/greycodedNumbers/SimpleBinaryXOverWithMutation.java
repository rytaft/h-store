package org.jaga.reproduction.greycodedNumbers;

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

public class SimpleBinaryXOverWithMutation extends CombinedReproductionAlgorithm {

	public SimpleBinaryXOverWithMutation() {
		super();
		insertReproductionAlgorithm(0, new SimpleBinaryMutation());
		insertReproductionAlgorithm(1, new SimpleBinaryXOver());
	}

	public SimpleBinaryXOverWithMutation(double xOverProb, double mutProb) {
		super();
		insertReproductionAlgorithm(0, new SimpleBinaryXOver(xOverProb));
		insertReproductionAlgorithm(1, new SimpleBinaryMutation(mutProb));
	}

	public void setXOverProbability(double xOverProb) {
		((SimpleBinaryXOver) getReproductionAlgorithm(0)).setXOverProbability(xOverProb);
	}

	public double getXOverProbability() {
		return ((SimpleBinaryXOver) getReproductionAlgorithm(0)).getXOverProbability();
	}

	public void setMutationProbability(double mutProb) {
		((SimpleBinaryMutation) getReproductionAlgorithm(1)).setMutationProbability(mutProb);
	}

	public double getMutationProbability() {
		return ((SimpleBinaryMutation) getReproductionAlgorithm(1)).getMutationProbability();
	}

}