package org.jaga.definitions;

/**
 * Basis for all aglowithms whichevaluate the fitness of an individual.
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

public interface FitnessEvaluationAlgorithm {

	/**
	 * Returns the class of individuals handled by this selection algorithm.
	 *
	 * @return The required type the individual passed to the
	 * method <code>evaluateFitness</code>.
	 */
	public Class getApplicableClass();

	/**
	 * Evaluates the fitness of a spscified individual.
	 *
	 * @param individual The Individual to be evaluated.
	 * @param age The generation of the individual and its population.
	 * @param population The population from which the individual was drawn.
	 * @param params The experiment-parameters
	 *
	 * @return The fitness of the specified individual
	 *
	 * @throws ClassCastException If this algorithm cannot handle individuals of
	 * the type of <code>individual</code>, i.e. the actual class of
	 * <code>individual</code> is not the same of a subclass of the class
	 * returned by <code>getApplicableClass()</code>.
	 */
	public Fitness evaluateFitness(Individual individual, int age,
								   Population population, GAParameterSet params)
														throws ClassCastException;

}