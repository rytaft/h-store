package org.jaga.definitions;



/**
 * Basis for all populations.
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

public interface Population {

	/**
	 * Gets the size of the population.
	 *
	 * @return The number of individuals in the population.
	 */
	public int getSize();

	/**
	 * Adds a list of individuals to the population.
	 *
	 * @param individuals An array of individuals which should be added to the#
	 * population.
	 */
	public void addAll(Individual [] individuals);

	/**
	 * Adds a specified individual to the population.
	 *
	 * @param individual To add to the population.
	 */
	public void add(Individual individual);

	/**
	 * Gets an array of all individuals in the population.
	 *
	 * @return An array containing all individuals in the population.
	 */
	public Individual [] getAllMembers();

	/**
	 * Gets an individual at the specifies index from the population.<br/>
	 * If this population does not support an ordering, this method may
	 * throw an <code>UnsupportedOperationException</code>.
	 *
	 * @param index The index of the population member to return.
	 *
	 * @return The member of the mopulation at the specified index.
	 *
	 * @throws ArrayIndexOutOfBoundsException If index is invalid.
	 */
	public Individual getMember(int index) throws ArrayIndexOutOfBoundsException;

}