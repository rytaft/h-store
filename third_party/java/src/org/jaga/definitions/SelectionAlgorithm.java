package org.jaga.definitions;

/**
 * Basis for all algorithms which perform selection for reproduction.
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

public interface SelectionAlgorithm {

	/**
	 * Selects an individual from the specified population according to the fitness.
	 *
	 * @param population The population to select from. All individuals in the
	 * population should return a fitness object which is possible to cast to
	 * the class returned by <code>getApplicableFitnessClass</code>.
	 *
	 * @param age The current generation of the population.
	 *
	 * @param params Experiment parameters.
	 *
	 * @return A selected individual.
	 *
	 * @throws ClassCastException If the population specified contains
	 * individuals which return a fitness-object, which cannot be cast to the
	 * class returned by <code>getApplicableFitnessClass</code>.
	 */
	public Individual select(Population population, int age,
							 GAParameterSet params) throws ClassCastException;

	/**
	 * Selects a spacified number of individuals from the population according
	 * to their fitness.
	 *
	 * @param population The population to select from. All individuals in the
	 * population should return a fitness object which is possible to cast to
	 * the class returned by <code>getApplicableFitnessClass</code>.
	 *
	 * @param howMany The numbetr of individuals to select.
	 *
	 * @param age The current generation of the population.
	 *
	 * @param params Experiment parameters.
	 *
	 * @return An array of selected individuals.
	 *
	 * @throws ClassCastException If the population specified contains
	 * individuals which return a fitness-object, which cannot be cast to the
	 * class returned by <code>getApplicableFitnessClass</code>.
	 */
	public Individual [] select(Population population, int howMany, int age,
								GAParameterSet params) throws ClassCastException;

	/**
	 * Gets the <code>Fitness</code>-class handles by this selector.
	 *
	 * @return A class, such that all individuals in the populations passed to
	 * the <code>select</code>-methods must have a fitness of that class or
	 * its subclass.
	 */
	public Class getApplicableFitnessClass();

}