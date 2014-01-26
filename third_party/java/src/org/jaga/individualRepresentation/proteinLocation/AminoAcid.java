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

public class AminoAcid extends PolypeptidePatternItem {

	private String name = null;
	private String code = null;
	private int properties = 0;

	private AminoAcid() {
		throw new UnsupportedOperationException("Use AminoAcid(String name, String code, int properties).");
	}

	/*package*/ AminoAcid(String name, String code, int properties) {
		super();
		this.name = name;
		this.code = code;
		this.properties = properties;
	}

	public boolean doesntMatch(AminoAcid aminoAcid) {
		return (this != aminoAcid);
	}

	public String getName() {
		return this.name;
	}

	public String getCode() {
		return this.code;
	}

	public String toString() {
		return this.getCode();
	}

	public int getProperties() {
		return this.properties;
	}

	public boolean isPolar() {
		return 0 != (this.properties & AminoAcidProperty.Polar);
	}

	public boolean isSmall() {
		return 0 != (this.properties & AminoAcidProperty.Small);
	}

	public boolean isHydrophobic() {
		return 0 != (this.properties & AminoAcidProperty.Hydrophobic);
	}

	public boolean isAliphatic() {
		return 0 != (this.properties & AminoAcidProperty.Aliphatic);
	}

	public boolean isAromatic() {
		return 0 != (this.properties & AminoAcidProperty.Aromatic);
	}

	public boolean isNegative() {
		return 0 != (this.properties & AminoAcidProperty.Negative);
	}

	public boolean isPositive() {
		return 0 != (this.properties & AminoAcidProperty.Positive);
	}

	public boolean isTiny() {
		return 0 != (this.properties & AminoAcidProperty.Tiny);
	}
}