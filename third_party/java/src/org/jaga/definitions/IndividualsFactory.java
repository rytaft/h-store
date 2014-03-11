package org.jaga.definitions;


/**
 * Basis for individual factories.
 * Individuals should not have a public constructor, but be rather constructed
 * by corresponding factories.
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

public interface IndividualsFactory {

	/**
	 * Creates a new default individual. For same factory settings, this method must
	 * always return the same individual (a new instance holding the same values),
	 * which is considered the defaut individual for those settings.
	 *
	 * @param params Experiment parameters.
	 *
	 * @return A new instance of the dafault individual for this factory's current
	 * settings.
	 */
	public Individual createDefaultIndividual(GAParameterSet params);

	/**
	 * Creates a random individual according to this factory's current internal
	 * settings and expariment paramanters.
	 *
	 * @param params Experiment parameters.
	 *
	 * @return A new instanc eof a randomly created individual.
	 */
	public Individual createRandomIndividual(GAParameterSet params);

	/**
	 * Creates a new individual and initialises it to the specified value(s).
	 * The parameter <code>init</code> can hold any value(s) which this factory
	 * knows to interprete.<br/>
	 * In particular, a factory should be able to handle
	 * <code>Individual</code>-objects of the same type as produced by the
	 * factory. If such an <code>Individual</code>-object is passed as the
	 * <code>init</code>-value, a deep copy of the individual should be created
	 * and returned by this method.<br/>
	 * Any other type is permited for the <code>init</code>-value, but the
	 * value should not be <code>null</code>. This method should create a new
	 * individual and initialise it with the value(s) packed encoded in
	 * <code>init</code>.
	 *
	 * @param init Initialisation value(s) for a new individual.
	 *
	 * @param params Experiment parameters.
	 *
	 * @return A new instance of an individual produced by this factory,
	 * initialised to the value(s) specified in <code>init</code>.
	 *
	 * @throws NullPointerException If <code>init</code> is <code>null</code>.
	 *
	 * @throws ClassCastException If <code>init</code> is of a type not supported
	 * by this factory for initalisation of individuals.
	 */
	public Individual createSpecificIndividual(Object init, GAParameterSet params)
									throws NullPointerException, ClassCastException;

}