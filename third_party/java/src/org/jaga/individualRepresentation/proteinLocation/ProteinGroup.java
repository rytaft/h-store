package org.jaga.individualRepresentation.proteinLocation;

import java.util.ArrayList;
import org.jaga.definitions.RandomGenerator;

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

public class ProteinGroup {

	private ArrayList proteins = new ArrayList();
	private ArrayList groups = new ArrayList();
	private String name = "<Untitled>";

	public ProteinGroup() {}

	public ProteinGroup(String name) {
		setName(name);
	}

	public ProteinGroup(String name, ProteinFileParser parser, String fileName) {
		setName(name);
		load(parser, fileName);
	}

	public void add(Protein prot) {
		this.proteins.add(prot);
	}

	public void add(ProteinGroup group) {
		this.groups.add(group);
	}

	public Protein getProtein(int index) {
		return (Protein) proteins.get(index);
	}

	public int size() {
		int s = 0;
		for (int i = 0; i < groups.size(); i++)
			s += ((ProteinGroup) groups.get(i)).size();
		s += proteins.size();
		return s;
	}

	public void flatten() {
		while (!groups.isEmpty()) {
			final int i = groups.size() - 1;
			ProteinGroup g = (ProteinGroup) groups.remove(i);
			g.flatten();
			for (int j = 0; j < g.size(); j++)
				proteins.add(g.getProtein(j));
		}
	}

	public void randomise(RandomGenerator rnd) {

		ArrayList newGroups = new ArrayList(groups.size());
		while(!groups.isEmpty()) {
			int i = rnd.nextInt(0, groups.size());
			ProteinGroup g = (ProteinGroup) groups.remove(i);
			newGroups.add(g);
			g.randomise(rnd);
		}
		groups = newGroups;

		ArrayList newProteins = new ArrayList(proteins.size());
		while(!proteins.isEmpty()) {
			int i = rnd.nextInt(0, proteins.size());
			Protein p = (Protein) proteins.remove(i);
			newProteins.add(p);
		}
		proteins = newProteins;
	}

	public int align(final PolypeptidePattern pattern, final int minOverlap) {
		int aligned = 0;
		aligned += alignInSubgroups(pattern, minOverlap);
		aligned += alignInProteins(pattern, minOverlap);
		return aligned;
	}

	private int alignInProteins(final PolypeptidePattern pattern, final int minOverlap) {
		int aligned = 0;
		for (int i = 0; i < proteins.size(); i++) {
			if (alignProtein((Protein) proteins.get(i), pattern, minOverlap))
				aligned++;
		}
		return aligned;
	}

	private boolean alignProtein(final Protein protein, final PolypeptidePattern pattern, final int minOverlap) {
		/*
		int pStop = protein.getLength() - pattern.getLength();
		for (int p = 0; p <= pStop; p++) {
			if (pattern.matches(protein, p))
				return true;
		}
		return false;
		*/
		// Use this speed hack:
		final int patLen = pattern.getLength();
		int pStart = -(patLen - minOverlap);
		if (pStart > 0)
			pStart = 0;
		final int pStop = protein.getLength() - minOverlap;
		for (int p = pStart; p <= pStop; p++) {
			if (pattern.matchesPerformanceHack(protein.getSequenceReferencePerformanceHack(), p))
				return true;
		}
		return false;
	}

	private int alignInSubgroups(final PolypeptidePattern pattern, final int minOverlap) {
		int aligned = 0;
		for (int i = 0; i < groups.size(); i++)
			aligned += ((ProteinGroup) groups.get(i)).align(pattern, minOverlap);
		return aligned;
	}

	public void load(ProteinFileParser parser, String fileName) {
		try {
			parser.readFromFile(fileName, this);
		} catch (java.io.IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		if (null == name)
			this.name = "<Untitled>";
		else
			this.name = name;
	}

}