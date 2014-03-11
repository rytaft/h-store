package org.jaga.individualRepresentation.proteinLocation;

import org.jaga.definitions.GAParameterSet;
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

public class AminoAcidPropertyFactory {

	public static final AminoAcidProperty Small = new AminoAcidProperty(AminoAcidProperty.Small);
	public static final AminoAcidProperty Hydrophobic = new AminoAcidProperty(AminoAcidProperty.Hydrophobic);
	public static final AminoAcidProperty Polar = new AminoAcidProperty(AminoAcidProperty.Polar);
	public static final AminoAcidProperty Positive = new AminoAcidProperty(AminoAcidProperty.Positive);
	public static final AminoAcidProperty Negative = new AminoAcidProperty(AminoAcidProperty.Negative);
	public static final AminoAcidProperty Tiny = new AminoAcidProperty(AminoAcidProperty.Tiny);
	public static final AminoAcidProperty Aliphatic = new AminoAcidProperty(AminoAcidProperty.Aliphatic);
	public static final AminoAcidProperty Aromatic = new AminoAcidProperty(AminoAcidProperty.Aromatic);

	public static AminoAcidProperty getRandomProperty(GAParameterSet params) {
		int dice = params.getRandomGenerator().nextInt(1, 9);
		switch(dice) {
			case 1: return getPropertyByCode(AminoAcidProperty.Small);
			case 2: return getPropertyByCode(AminoAcidProperty.Hydrophobic);
			case 3: return getPropertyByCode(AminoAcidProperty.Polar);
			case 4: return getPropertyByCode(AminoAcidProperty.Positive);
			case 5: return getPropertyByCode(AminoAcidProperty.Negative);
			case 6: return getPropertyByCode(AminoAcidProperty.Tiny);
			case 7: return getPropertyByCode(AminoAcidProperty.Aliphatic);
			case 8: return getPropertyByCode(AminoAcidProperty.Aromatic);
			default: throw new Error("Should never come here. Have fun debugging!");
		}
	}

	public static AminoAcidProperty getPropertyByCode(int propertyCode) {
		switch(propertyCode) {
			case AminoAcidProperty.Small:
				return Small;
			case AminoAcidProperty.Hydrophobic:
				return Hydrophobic;
			case AminoAcidProperty.Polar:
				return Polar;
			case AminoAcidProperty.Positive:
				return Positive;
			case AminoAcidProperty.Negative:
				return Negative;
			case AminoAcidProperty.Tiny:
				return Tiny;
			case AminoAcidProperty.Aliphatic:
				return Aliphatic;
			case AminoAcidProperty.Aromatic:
				return Aromatic;
			default:
				throw new IllegalArgumentException("Illegal property: " + propertyCode);
		}
	}

	public static AminoAcidProperty getPropertyByName(String name) {
		if (name.equalsIgnoreCase("Small"))
			return Small;
		if (name.equalsIgnoreCase("Hydrophobic"))
			return Hydrophobic;
		if (name.equalsIgnoreCase("Polar"))
			return Polar;
		if (name.equalsIgnoreCase("Positive"))
			return Positive;
		if (name.equalsIgnoreCase("Negative"))
			return Negative;
		if (name.equalsIgnoreCase("Tiny"))
			return Tiny;
		if (name.equalsIgnoreCase("Aliphatic"))
			return Aliphatic;
		if (name.equalsIgnoreCase("Aromatic"))
			return Aromatic;

		throw new IllegalArgumentException("Illegal property: " + name);
	}

	public AminoAcidPropertyFactory() {}

}