package org.jaga.individualRepresentation.proteinLocation;

import java.util.ArrayList;


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

public class PolypeptidePattern {

	/*
	Structure (in order of preference):
	- exact amino acid to match: e.g. ...-M-T-P-...
	- some amino posessing a given property to match: ...-<Small>-<Hydrophobic>-<Polar>-<Positive>-<Negative>-<Tiny>-<Aliphatic>-<Aromatic>-...
	- some amino acid out of a given list to match: e.g. ...-[AG]-[DE]-[KR]-...
	- any amino acid matches: ?
	*/

	private ArrayList items = new ArrayList();

	public PolypeptidePattern() {}

	public void clearAllItems() {
		items.clear();
	}

	public int getLength() {
		return items.size();
	}

	public PolypeptidePatternItem getItem(int index) {
		return (PolypeptidePatternItem) items.get(index);
	}

	public void removeItem(int index) {
		items.remove(index);
	}

	public void replaceItem(int index, PolypeptidePatternItem item) {
		if (null == item)
			throw new NullPointerException("Debug!");
		if (item instanceof AminoAcidGroup && ((AminoAcidGroup) item).size() == 0)
			throw new RuntimeException("Debug!");
		items.set(index, item);
	}

	public void insertItem(int index, PolypeptidePatternItem item) {
		if (null == item)
			throw new NullPointerException("Debug!");
		if (item instanceof AminoAcidGroup && ((AminoAcidGroup) item).size() == 0)
			throw new RuntimeException("Debug!");
		items.add(index, item);
	}

	public void insertItem(PolypeptidePatternItem item) {
		if (null == item)
			throw new NullPointerException("Debug!");
		if (item instanceof AminoAcidGroup && ((AminoAcidGroup) item).size() == 0)
			throw new RuntimeException("Debug!");
		items.add(item);
	}

	public boolean matches(Protein prot, int fromLocation) {
		return false;
		/* BRING UP DO DATE WITH THE FAST VERSION BEFORE UNCOMMENTING!!
		int protLen = prot.getLength();
		int patternLen = items.size();

		if (protLen - patternLen < fromLocation)
			return false;

		int location = fromLocation;
		PolypeptidePatternItem patItem;
		AminoAcid protResidue;
		for (int i = 0; i < patternLen; i++, location++) {
			patItem = (PolypeptidePatternItem) items.get(i);
			protResidue = prot.getResidue(location);
			if (patItem.doesntMatch(protResidue))
				return false;
		}
		return true;
		*/
	}

	public boolean matchesPerformanceHack(AminoAcid [] sequence, int fromLocation) {

		final int patternLen = items.size();
		final int protLen = sequence.length;
		PolypeptidePatternItem patItem;
		int location = 0;
		int i = (fromLocation < 0 ? -fromLocation : 0);
		while (i < patternLen && location < protLen) {
			patItem = (PolypeptidePatternItem) items.get(i++);
			if (patItem.doesntMatch(sequence[location++]))
				return false;
		}
		return true;
	}

	public String toString() {
		StringBuffer s = new StringBuffer("{");
		for (int i = 0; i < items.size(); i++) {
			if (i > 0)
				s.append("-");
			s.append(items.get(i).toString());
		}
		s.append("}");
		return s.toString();
	}

}