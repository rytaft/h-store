package org.jaga.individualRepresentation.proteinLocation;

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

public class ProteinLocationClassifier implements Individual {

	private PolypeptidePattern pattern = null;
	private Fitness fitness = null;
	private String name = "<untitled>";


	private ProteinLocationClassifier() {
		throw new UnsupportedOperationException("Use ProteinLocationClassifier(PolypeptidePattern pattern) instead");
	}

	public ProteinLocationClassifier(PolypeptidePattern pattern, String name) {
		this.pattern = pattern;
		this.name = name;
	}

	public PolypeptidePattern getPattern() {
		return this.pattern;
	}

	public void setPattern(PolypeptidePattern pattern) {
		this.pattern = pattern;
	}

	public Fitness getFitness() {
		return this.fitness;
	}

	public void setFitness(Fitness fitness) {
		this.fitness = fitness;
	}

	public String toString() {
		StringBuffer s = new StringBuffer("Pattern=");
		s.append(pattern.toString());
		s.append(". Fitness=");
		s.append(fitness.toString());
		s.append(".");
		return s.toString();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
