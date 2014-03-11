package org.jaga.individualRepresentation.proteinLocation;


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

public class AminoAcidProperty extends PolypeptidePatternItem {

	public static final int Small = 1;
	public static final int Hydrophobic = 2;
	public static final int Polar = 4;
	public static final int Positive = 8;
	public static final int Negative = 16;
	public static final int Tiny = 32;
	public static final int Aliphatic = 64;
	public static final int Aromatic = 128;

	private int property = 0;
	private String name = null;

	private AminoAcidProperty() {
		throw new UnsupportedOperationException("Use AminoAcidProperty(short property) instead.");
	}

	/*package*/ AminoAcidProperty(int property) {
		super();
		switch(property) {
			case Small:
				this.property = Small;
				this.name = "Small";
				break;
			case Hydrophobic:
				this.property = Hydrophobic;
				this.name = "Hydrophobic";
				break;
			case Polar:
				this.property = Polar;
				this.name = "Polar";
				break;
			case Positive:
				this.property = Positive;
				this.name = "Positive";
				break;
			case Negative:
				this.property = Negative;
				this.name = "Negative";
				break;
			case Tiny:
				this.property = Tiny;
				this.name = "Tiny";
				break;
			case Aliphatic:
				this.property = Aliphatic;
				this.name = "Aliphatic";
				break;
			case Aromatic:
				this.property = Aromatic;
				this.name = "Aromatic";
				break;
			default:
				throw new IllegalArgumentException("Illegal property: " + property);
		}

	}

	public int getProperty() {
		return this.property;
	}

	public String toString() {
		StringBuffer s = new StringBuffer("<");
		s.append(name);
		s.append(">");
		return s.toString();
	}

	public boolean doesntMatch(AminoAcid aminoAcid) {
		int itemProperties = aminoAcid.getProperties();
		return 0 == (itemProperties & this.property);
	}

}