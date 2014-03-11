package org.jaga.reproduction;

import java.util.ArrayList;
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

public class CombinedReproductionAlgorithm implements ReproductionAlgorithm {

	private ArrayList combinedAlgorithms = new ArrayList(2);
	private Class applicableClass = Individual.class;

	public CombinedReproductionAlgorithm() {
		super();
	}

	public void insertReproductionAlgorithm(int position, ReproductionAlgorithm algorithm) {
		if (null == algorithm)
			throw new NullPointerException("ReproductionAlgorithm may not be null");
		if (position < 0 || combinedAlgorithms.size() < position)
			throw new NullPointerException("Position is invalid (" + position + ")");
		checkAlgorithmFitsTheOthers(algorithm);
		combinedAlgorithms.add(position, algorithm);
		updateApplicableClass();
	}

	public void setReproductionAlgorithm(int position, ReproductionAlgorithm algorithm) {
		if (null == algorithm)
			throw new NullPointerException("ReproductionAlgorithm may not be null");
		if (position < 0 || combinedAlgorithms.size() <= position)
			throw new NullPointerException("Position is invalid (" + position + ")");
		checkAlgorithmFitsTheOthers(algorithm);
		combinedAlgorithms.set(position, algorithm);
		updateApplicableClass();
	}

	public int findReproductionAlgorithm(ReproductionAlgorithm algorithm) {
		if (null == algorithm)
			throw new NullPointerException("ReproductionAlgorithm may not be null");
		return combinedAlgorithms.indexOf(algorithm);
	}

	public void removeReproductionAlgorithm(int position) {
		if (position < 0 || combinedAlgorithms.size() <= position)
			throw new NullPointerException("Position is invalid (" + position + ")");
		combinedAlgorithms.remove(position);
		updateApplicableClass();
	}

	public ReproductionAlgorithm getReproductionAlgorithm(int position) {
		if (position < 0 || combinedAlgorithms.size() <= position)
			throw new NullPointerException("Position is invalid (" + position + ")");
		return (ReproductionAlgorithm) combinedAlgorithms.get(position);
	}

	public int countCombinedAlgorithms() {
		return combinedAlgorithms.size();
	}

	private void checkAlgorithmFitsTheOthers(ReproductionAlgorithm algorithm)
															  throws IllegalArgumentException {
		if (!getApplicableClass().isAssignableFrom(algorithm.getApplicableClass()))
			throw new IllegalArgumentException("Incompatible ReproductionAlgorithm: "
											   + "specified algorithm is of type "
											   + algorithm.getClass().getName()
											   + ", but must be of type "
											   + getApplicableClass().getName());
		if (0 > algorithm.getRequiredNumberOfParents())
			return;
		for (int i = 0; i < combinedAlgorithms.size(); i++) {
			int p = ((ReproductionAlgorithm) combinedAlgorithms.get(i)).getRequiredNumberOfParents();
			if (0 > p)
				continue;
			if (algorithm.getRequiredNumberOfParents() != p)
				throw new IllegalArgumentException("Incompatible ReproductionAlgorithm: "
												   + "should require " + p + " parents, "
												   + "but requires "
												   + algorithm.getRequiredNumberOfParents());
		}
	}

	private void updateApplicableClass() {
		if (0 == combinedAlgorithms.size()) {
			applicableClass = Individual.class;
		} else {
			Class type = getReproductionAlgorithm(0).getApplicableClass();
			for (int i = 1; i < countCombinedAlgorithms(); i++) {
				Class t = getReproductionAlgorithm(i).getApplicableClass();
				if (t != type && t.isAssignableFrom(type))
					type = t;
			}
			applicableClass = type;
		}
	}

	public Class getApplicableClass() {
		return applicableClass;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {
		Individual [] kids = parents;
		for (int i = 0; i < combinedAlgorithms.size(); i++) {
			ReproductionAlgorithm alg = (ReproductionAlgorithm) combinedAlgorithms.get(i);
			kids = alg.reproduce(kids, params);
		}
		return kids;
	}

	public int getRequiredNumberOfParents() {
		if (0 == combinedAlgorithms.size())
			return -1;
		for (int i = 0; i < combinedAlgorithms.size(); i++) {
			int p = ((ReproductionAlgorithm) combinedAlgorithms.get(i)).getRequiredNumberOfParents();
			if (0 <= p)
				return p;
		}
		return -1;
	}

}