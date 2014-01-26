package org.jaga.definitions;

/**
 * 
 * Basis for all classes which implement the fitness of an individual.
 * Every individual has a property of this type (its subtype), which describes
 * how fit that indivudual is compared to others.
 * It must be possible to compare two fitness-objects. However, the fitnesses
 * of all indivuduals in the population are not nessecerily all comparable:
 * for example, it is possible that the fitness is determined in an N-Tournament
 * selection process. According to the nature of that tournament the fitness
 * might not be absolut, i.e. fitness doen not have to be transitive:
 * A &lt; B and B &lt; C does not always mean A &lt;. In particular, this
 * applies to fitnesses of game strategies.
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
 * @version JAGA public release 1.0 Beta
 */
public interface Fitness {
	
	/**
	 * Compares this fitness to the specified fitness.
	 * 
	 * @param fitness - another fitness object.
	 * @return true, if this fitness is "fitter" then the specified,
	 * false otherwise.
	 * @throws ClassCastException if these types of fitness are not comparable.
	 */
	public boolean isBetter(Fitness fitness) throws ClassCastException;
	
	/**
	 * Compares this fitness to the specified fitness.
	 * 
	 * @param fitness - another fitness object.
	 * @return true, if this fitness is "less fit" then the specified,
	 * false otherwise.
	 * @throws ClassCastException if these types of fitness are not comparable.
	 */
	public boolean isWorse(Fitness fitness) throws ClassCastException;
	
}