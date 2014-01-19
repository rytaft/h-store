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

public class AminoAcidGroup extends PolypeptidePatternItem {

	private ArrayList acids = new ArrayList();
	private int countSmall = 0;
	private int countHydrophobic = 0;
	private int countPolar = 0;
	private int countPositive = 0;
	private int countNegative = 0;
	private int countTiny = 0;
	private int countAliphatic = 0;
	private int countAromatic = 0;

	public AminoAcidGroup() {}

	public boolean doesntMatch(AminoAcid aminoAcid) {
		return !acids.contains(aminoAcid);
	}

	public AminoAcid getResidue(int index) {
		return (AminoAcid) this.acids.get(index);
	}

	public void swapResidue(int index, AminoAcid newAminoAcid) {
		if (acids.contains(newAminoAcid))
			return;
		removeResidue(index);
		addResidue(newAminoAcid);
	}

	public AminoAcid removeResidue(int index) {
		AminoAcid aa = (AminoAcid) this.acids.remove(index);
		removeAcidFromStats(aa);
		return aa;
	}

	public void addResidue(AminoAcid aminoAcid) {
		if (acids.contains(aminoAcid))
			return;
		acids.add(aminoAcid);
		addAcidToStats(aminoAcid);
	}

	public int size() {
		return this.acids.size();
	}

	private void addAcidToStats(AminoAcid aminoAcid) {
		if (aminoAcid.isSmall())        countSmall++;
		if (aminoAcid.isHydrophobic())  countHydrophobic++;
		if (aminoAcid.isPolar())        countPolar++;
		if (aminoAcid.isPositive())     countPositive++;
		if (aminoAcid.isNegative())     countNegative++;
		if (aminoAcid.isTiny())         countTiny++;
		if (aminoAcid.isAliphatic())    countAliphatic++;
		if (aminoAcid.isAromatic())     countAromatic++;
	}

	private void removeAcidFromStats(AminoAcid aminoAcid) {
		if (aminoAcid.isSmall())        countSmall--;
		if (aminoAcid.isHydrophobic())  countHydrophobic--;
		if (aminoAcid.isPolar())        countPolar--;
		if (aminoAcid.isPositive())     countPositive--;
		if (aminoAcid.isNegative())     countNegative--;
		if (aminoAcid.isTiny())         countTiny--;
		if (aminoAcid.isAliphatic())    countAliphatic--;
		if (aminoAcid.isAromatic())     countAromatic--;
	}

	public double getPropertyProportion(int property) {
		int count = 0;
		if (0 != (property & AminoAcidProperty.Small))
			count += countSmall;
		if (0 != (property & AminoAcidProperty.Hydrophobic))
			count += countHydrophobic;
		if (0 != (property & AminoAcidProperty.Polar))
			count += countPolar;
		if (0 != (property & AminoAcidProperty.Positive))
			count += countPositive;
		if (0 != (property & AminoAcidProperty.Negative))
			count += countNegative;
		if (0 != (property & AminoAcidProperty.Tiny))
			count += countTiny;
		if (0 != (property & AminoAcidProperty.Aliphatic))
			count += countAliphatic;
		if (0 != (property & AminoAcidProperty.Aromatic))
			count += countAromatic;
		return ((double) count) / ((double) acids.size());
	}

	public int getMaxRepresentedProperty() {
		double percent = 0;
		int property = 0;
		if (getPropertyProportion(AminoAcidProperty.Small) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Small);
			property = AminoAcidProperty.Small;
		}
		if (getPropertyProportion(AminoAcidProperty.Hydrophobic) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Hydrophobic);
			property = AminoAcidProperty.Hydrophobic;
		}
		if (getPropertyProportion(AminoAcidProperty.Polar) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Polar);
			property = AminoAcidProperty.Polar;
		}
		if (getPropertyProportion(AminoAcidProperty.Positive) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Positive);
			property = AminoAcidProperty.Positive;
		}
		if (getPropertyProportion(AminoAcidProperty.Negative) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Negative);
			property = AminoAcidProperty.Negative;
		}
		if (getPropertyProportion(AminoAcidProperty.Tiny) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Tiny);
			property = AminoAcidProperty.Tiny;
		}
		if (getPropertyProportion(AminoAcidProperty.Aliphatic) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Aliphatic);
			property = AminoAcidProperty.Aliphatic;
		}
		if (getPropertyProportion(AminoAcidProperty.Aromatic) > percent) {
			percent = getPropertyProportion(AminoAcidProperty.Aromatic);
			property = AminoAcidProperty.Aromatic;
		}
		return property;
	}

	public String toString() {
		StringBuffer s = new StringBuffer("[");
		for (int i = 0; i < acids.size(); i++)
			s.append(acids.get(i).toString());
		s.append("]");
		return s.toString();
	}

}