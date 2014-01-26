package org.jaga.util;

import java.util.Random;
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

public class DefaultRandomGenerator extends Random implements RandomGenerator {

	public DefaultRandomGenerator() {
	}

	public DefaultRandomGenerator(long seed) {
		super(seed);
	}

	public byte nextByte() {
		return (byte) nextDouble(Byte.MIN_VALUE, Byte.MAX_VALUE);
	}

	public byte nextByte(byte minInclusive, byte maxExclusive) {
		return (byte) nextDouble(minInclusive, maxExclusive);
	}

	public int nextInt(int minInclusive, int maxExclusive) {
		return (int) nextDouble(minInclusive, maxExclusive);
	}

	public long nextLong(long minInclusive, long maxExclusive) {
		return (long) nextDouble(minInclusive, maxExclusive);
	}

	public float nextFloat(float minInclusive, float maxExclusive) {
		return (float) nextDouble(minInclusive, maxExclusive);
	}

	public double nextDouble(double minInclusive, double maxExclusive) {
		if (!(minInclusive < maxExclusive))
			throw new IllegalArgumentException("Required that minInclusive < maxExclusive (but "
											   + minInclusive + " >= " + maxExclusive + ")");
		double rnd = nextDouble() - 0.5;
		// double range = maxExclusive - minInclusive;
		// return minInclusive + rnd * range;
		double halfRange = maxExclusive * 0.5 - minInclusive * 0.5;
		return minInclusive + halfRange + (2.0 * rnd) * halfRange;
	}

	public double nextGaussian(double mean, double stdDev) {
		double rnd = nextGaussian();
		return mean + rnd * stdDev;
	}

}