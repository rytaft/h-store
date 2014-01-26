package org.jaga.definitions;


/**
 * Basis for all Random Number Generators to be used in JAGA.
 * JAGA uses an RNG specified in parameters rather the a system RNG. This
 * allowes to reproduce results by specifying a predictable RNG (e.g. by
 * using the same seed for the Java-RNG).
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

public interface RandomGenerator {

	public void setSeed(long seed);

	public boolean nextBoolean();

	public byte nextByte();
	public byte nextByte(byte minInclusive, byte maxExclusive);

	public int nextInt();
	public int nextInt(int minInclusive, int maxExclusive);

	public long nextLong();
	public long nextLong(long minInclusive, long maxExclusive);

	public float nextFloat();
	public float nextFloat(float minInclusive, float maxExclusive);

	public double nextDouble();
	public double nextDouble(double minInclusive, double maxExclusive);

	public double nextGaussian();
	public double nextGaussian(double mean, double stdDev);
}