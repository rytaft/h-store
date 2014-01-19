package org.jaga.individualRepresentation.proteinLocation;

import org.jaga.definitions.GAParameterSet;
import org.jaga.definitions.RandomGenerator;
import java.util.HashMap;

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

public class AminoAcidFactory {

	private static HashMap aminoAcids = new HashMap();
	private static AminoAcid [] allAminoAcids = null;
	private static AminoAcid [] smallAminoAcids = null;
	private static AminoAcid [] hydrophobicAminoAcids = null;
	private static AminoAcid [] polarAminoAcids = null;
	private static AminoAcid [] positiveAminoAcids = null;
	private static AminoAcid [] negativeAminoAcids = null;
	private static AminoAcid [] tinyAminoAcids = null;
	private static AminoAcid [] aliphaticAminoAcids = null;
	private static AminoAcid [] aromaticAminoAcids = null;

	static {

		AminoAcid I = new AminoAcid("Isoleukine", "I",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Aliphatic);

		AminoAcid L = new AminoAcid("Leucine", "L",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Aliphatic);

		AminoAcid M = new AminoAcid("Methionine", "M",
					   AminoAcidProperty.Hydrophobic);

		AminoAcid F = new AminoAcid("Phelylalanine", "F",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Aromatic);

		AminoAcid A = new AminoAcid("Alanine", "A",
						AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small | AminoAcidProperty.Tiny);

		AminoAcid C = new AminoAcid("Cysteine", "C",
						AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small);

		AminoAcid V = new AminoAcid("Valine", "V",
						AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small | AminoAcidProperty.Aliphatic);

		AminoAcid T = new AminoAcid("Threonine", "T",
						AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small | AminoAcidProperty.Polar);

		AminoAcid W = new AminoAcid("Tryptophan", "W",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small | AminoAcidProperty.Aromatic);

		AminoAcid Y = new AminoAcid("Tyrosine", "Y",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small | AminoAcidProperty.Aromatic);

		AminoAcid H = new AminoAcid("Histidine", "H",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Polar | AminoAcidProperty.Positive | AminoAcidProperty.Aromatic);

		AminoAcid K = new AminoAcid("Lysine", "K",
					   AminoAcidProperty.Hydrophobic | AminoAcidProperty.Polar | AminoAcidProperty.Positive);

		AminoAcid R = new AminoAcid("Arginine", "R",
					   AminoAcidProperty.Polar | AminoAcidProperty.Positive);

		AminoAcid E = new AminoAcid("Glutamic Acid", "E",
					   AminoAcidProperty.Polar | AminoAcidProperty.Negative);

		AminoAcid D = new AminoAcid("Aspatic Acid", "D",
						AminoAcidProperty.Small | AminoAcidProperty.Polar | AminoAcidProperty.Positive);

		AminoAcid N = new AminoAcid("Asparagine", "N",
						AminoAcidProperty.Small | AminoAcidProperty.Polar);

		AminoAcid G = new AminoAcid("Glycine", "G",
						AminoAcidProperty.Hydrophobic | AminoAcidProperty.Small | AminoAcidProperty.Tiny);

		AminoAcid S = new AminoAcid("Serine", "S",
						AminoAcidProperty.Polar | AminoAcidProperty.Small | AminoAcidProperty.Tiny);

		AminoAcid Q = new AminoAcid("Glutamine", "Q",
					   AminoAcidProperty.Polar);

		AminoAcid P = new AminoAcid("Proline", "P",
						AminoAcidProperty.Small);


		aminoAcids.put("I", I);
		aminoAcids.put("L", L);
		aminoAcids.put("M", M);
		aminoAcids.put("F", F);
		aminoAcids.put("A", A);
		aminoAcids.put("C", C);
		aminoAcids.put("V", V);
		aminoAcids.put("T", T);
		aminoAcids.put("W", W);
		aminoAcids.put("Y", Y);
		aminoAcids.put("H", H);
		aminoAcids.put("K", K);
		aminoAcids.put("R", R);
		aminoAcids.put("E", E);
		aminoAcids.put("D", D);
		aminoAcids.put("N", N);
		aminoAcids.put("G", G);
		aminoAcids.put("S", S);
		aminoAcids.put("Q", Q);
		aminoAcids.put("P", P);

		allAminoAcids = new AminoAcid [] {I, L, M, F, A, C, V, T, W, Y, H, K, R, E, D, N, G, S, Q, P};
		smallAminoAcids = new AminoAcid [] {P, N, D, T, V, C, G, A, S};
		hydrophobicAminoAcids = new AminoAcid [] {K, H, Y, W, F, M, L, V, I, C, T, A, G};
		polarAminoAcids = new AminoAcid [] {Y, W, H, R, K, T, D, E, S, N, Q};
		positiveAminoAcids = new AminoAcid [] {K, R, H};
		negativeAminoAcids = new AminoAcid [] {D, E};
		tinyAminoAcids = new AminoAcid [] {S, A, G};
		aliphaticAminoAcids = new AminoAcid [] {V, L, I};
		aromaticAminoAcids = new AminoAcid [] {F, Y, W, H};
	}

	public static AminoAcid getResidueByCode(String code) {
		Object aa = aminoAcids.get(code);
		if (null == aa)
			throw new RuntimeException("There is no amino acid with the code '" + code + "'.");
		return (AminoAcid) aa;
	}

	public static AminoAcid getRandomResidueByProperty(int property, GAParameterSet params) {
		switch(property) {
			case AminoAcidProperty.Small:
				return getRandomAcid(smallAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Hydrophobic:
				return getRandomAcid(hydrophobicAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Polar:
				return getRandomAcid(polarAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Positive:
				return getRandomAcid(positiveAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Negative:
				return getRandomAcid(negativeAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Tiny:
				return getRandomAcid(tinyAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Aliphatic:
				return getRandomAcid(aliphaticAminoAcids, params.getRandomGenerator());
			case AminoAcidProperty.Aromatic:
				return getRandomAcid(aromaticAminoAcids, params.getRandomGenerator());
			default:
				throw new Error("Cannot have property code " + property);
		}
	}

	public static AminoAcid getRandomResidue(GAParameterSet params) {
		return getRandomAcid(allAminoAcids, params.getRandomGenerator());
	}

	private static AminoAcid getRandomAcid(AminoAcid [] list, RandomGenerator rnd) {
		int dice = rnd.nextInt(0, list.length);
		return list[dice];
	}

	public AminoAcidFactory() {}

}