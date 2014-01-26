package org.jaga.definitions;


/**
 * Basis for all algorithms which perform the reproduction of individuals.
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

public interface ReproductionAlgorithm {

	/**
	 * Returns, how many individuals are required to produce one new individual
	 * whwn using this reproduction algorithm. If any number of parents is
	 * permitted (e.g. for most kinds of mutation), this method should
	 * return <code>-1</code>.
	 *
	 * @return The required length of the array-parameter <code>parents</code> to the
	 * method <code>reproduce</code> or <code>-1</code> if any length is parmitted.
	 *
	 */
	public int getRequiredNumberOfParents();

	/**
	 * Returns the class of individuals handled by this reproduction algorithm.
	 *
	 * @return The required type of the array-members in the parameter
	 * <code>parents</code> to the method <code>ReproductionAlgorithm.reproduce</code>.
	 */
	public Class getApplicableClass();

	/**
	 * Reproduces the specified individuals.
	 *
	 * @param parents The individuals to reproduce. The type of objects in this
	 * array must be the class returned by <code>getApplicableClass</code>.
	 * The number of objects in this array must be the same as returned by
	 * <code>getRequiredNumberOfParents</code>.
	 *
	 * @param params Experiment parameters.
	 *
	 * @return The children - i.e. the result of the reproduction of
	 * <code>parents</code>.
	 *
	 * @throws ClassCastException If a parent is not of a class which can be
	 * cast to the class returned by <code>getApplicableClass</code>.
	 *
	 * @throws ClassCastException If the number of parents does not correspond
	 * to the required number obtained via <code>getRequiredNumberOfParents</code>.
	 */
	public Individual [] reproduce(Individual [] parents, GAParameterSet params)
							throws ClassCastException, IllegalArgumentException;

}