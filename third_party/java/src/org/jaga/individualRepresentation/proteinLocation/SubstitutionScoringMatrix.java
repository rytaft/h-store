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

public abstract class SubstitutionScoringMatrix {

	private static final AminoAcid [] allAcids = new AminoAcid[] {
			AminoAcidFactory.getResidueByCode("I"), AminoAcidFactory.getResidueByCode("L"),
			AminoAcidFactory.getResidueByCode("M"), AminoAcidFactory.getResidueByCode("F"),
			AminoAcidFactory.getResidueByCode("A"), AminoAcidFactory.getResidueByCode("C"),
			AminoAcidFactory.getResidueByCode("V"), AminoAcidFactory.getResidueByCode("T"),
			AminoAcidFactory.getResidueByCode("W"), AminoAcidFactory.getResidueByCode("Y"),
			AminoAcidFactory.getResidueByCode("H"), AminoAcidFactory.getResidueByCode("K"),
			AminoAcidFactory.getResidueByCode("R"), AminoAcidFactory.getResidueByCode("E"),
			AminoAcidFactory.getResidueByCode("D"), AminoAcidFactory.getResidueByCode("N"),
			AminoAcidFactory.getResidueByCode("G"), AminoAcidFactory.getResidueByCode("S"),
			AminoAcidFactory.getResidueByCode("Q"), AminoAcidFactory.getResidueByCode("P"),
			null};

	public SubstitutionScoringMatrix() {}

	public abstract int getLogScore(AminoAcid aminoAcid1, AminoAcid aminoAcid2);
	public abstract double getActualScore(AminoAcid aminoAcid1, AminoAcid aminoAcid2);

	public int getLogScore(String aminoAcid1, String aminoAcid2) {
		AminoAcid a1 = (null == aminoAcid1 ? null : AminoAcidFactory.getResidueByCode(aminoAcid1));
		AminoAcid a2 = (null == aminoAcid2 ? null : AminoAcidFactory.getResidueByCode(aminoAcid2));
		return getLogScore(a1, a2);
	}

	public double getActualScore(String aminoAcid1, String aminoAcid2) {
		AminoAcid a1 = (null == aminoAcid1 ? null : AminoAcidFactory.getResidueByCode(aminoAcid1));
		AminoAcid a2 = (null == aminoAcid2 ? null : AminoAcidFactory.getResidueByCode(aminoAcid2));
		return getActualScore(a1, a2);
	}

	public AminoAcid chooseProbabalisticMutation(AminoAcid aminoAcid, GAParameterSet params) {
		double [] probs = new double[21];
		double sum = 0.;
		for (int i = 0; i < 21; i++) {
			probs[i] = getActualScore(aminoAcid, allAcids[i]);
			sum += probs[i];
		}

		double dice = params.getRandomGenerator().nextDouble(0, sum);
		double lower = 0.;
		double upper = 0.;
		for (int i = 0; i < 21; i++) {
			upper += probs[i];
			if (lower <= dice && dice < upper) {
				return allAcids[i];
			}
			lower = upper;
		}

		throw new Error("This line should never be executed. Hafe fun debugging!");
	}

	public AminoAcid chooseProbabalisticMutation(String aminoAcid, GAParameterSet params) {
		return chooseProbabalisticMutation(AminoAcidFactory.getResidueByCode(aminoAcid), params);
	}

}