package org.jaga.exampleApplications.proteinLocation;

import org.jaga.individualRepresentation.proteinLocation.*;
import org.jaga.selection.*;

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

public class Locator {

	private static final int minOverlap = 3;

	private static final int A = 0;
	private static final int B = 1;
	private static final int C = 2;
	private static final int D = 3;
	private static final String [] typeNames = new String [] {"Cytosol",
															  "Nucleus",
															  "Mitochondrion",
															  "Extracellular"};

	private ProteinLocationClassifier [] classifiers = new ProteinLocationClassifier[D+1];
	private int totalClassifications[] = new int[D+1];

	public Locator() {}

	public void setClassifiers(ProteinLocationClassifier cytosol,
							   ProteinLocationClassifier nucleus,
							   ProteinLocationClassifier mitochond,
							   ProteinLocationClassifier extracell) {
		this.classifiers[A] = cytosol;
		this.classifiers[B] = nucleus;
		this.classifiers[C] = mitochond;
		this.classifiers[D] = extracell;
	}

	private double sensitivity(int i) {
		return ((ClassifierFitness) classifiers[i].getFitness()).getSensitivity();
	}

	private double specificity(int i) {
		return ((ClassifierFitness) classifiers[i].getFitness()).getSpecificity();
	}

	private int analyseResults(Protein prot, boolean [] match) {

		// Get the basic probability assignments:
		double [] m = new double[]{0, 0, 0, 0};
		double [] mNOT = new double[]{0, 0, 0, 0};
		double [] doubt = new double[]{0, 0, 0, 0};
		for (int i = A; i <= D; i++) {
			if (match[i]) {
				m[i] = sensitivity(i);
				doubt[i] = 1.0 - m[i];
			} else {
				mNOT[i] = sensitivity(i);
				doubt[i] = 1.0 - mNOT[i];
			}
		}

		// Get combined probability assignments:

		/*
		double cmEmpty = m[A] * m[B] + m[A] * m[C] + m[A] * m[D]
					   + m[B] * m[C] + m[B] * m[D]
					   + m[C] * m[D];

		double norm = 1.0 - cmEmpty;

		double cmA = m[A] * mNOT[B]  + m[A] * mNOT[C]  + m[A] * mNOT[D]
				   + m[A] * doubt[B] + m[A] * doubt[C] + m[A] * doubt[D];
		cmA /= norm;

		double cmB = m[B] * mNOT[A]  + m[B] * mNOT[C]  + m[B] * mNOT[D]
				   + m[B] * doubt[A] + m[B] * doubt[C] + m[B] * doubt[D];
		cmB /= norm;

		double cmC = m[C] * mNOT[A]  + m[C] * mNOT[B]  + m[C] * mNOT[D]
				   + m[C] * doubt[A] + m[C] * doubt[B] + m[C] * doubt[D];
		cmC /= norm;

		double cmD = m[D] * mNOT[A]  + m[D] * mNOT[B]  + m[D] * mNOT[C]
				   + m[D] * doubt[A] + m[D] * doubt[B] + m[D] * doubt[C];
		cmD /= norm;

		double cmAB = (mNOT[C] * mNOT[B]) / norm;
		double cmAC = (mNOT[B] * mNOT[D]) / norm;
		double cmAD = (mNOT[B] * mNOT[C]) / norm;
		double cmBC = (mNOT[A] * mNOT[D]) / norm;
		double cmBD = (mNOT[A] * mNOT[C]) / norm;
		double cmCD = (mNOT[A] * mNOT[B]) / norm;

		double cmABC = (mNOT[D] * doubt[A] + mNOT[D] * doubt[B] + mNOT[D] * doubt[C]) / norm;
		double cmABD = (mNOT[C] * doubt[A] + mNOT[C] * doubt[B] + mNOT[C] * doubt[D]) / norm;
		double cmACD = (mNOT[B] * doubt[A] + mNOT[B] * doubt[C] + mNOT[B] * doubt[D]) / norm;
		double cmBCD = (mNOT[A] * doubt[B] + mNOT[A] * doubt[C] + mNOT[A] * doubt[D]) / norm;

		double cmABCD = doubt[A] * doubt[B] + doubt[A] * doubt[C] + doubt[A] * doubt[D]
					  + doubt[B] * doubt[C] + doubt[B] * doubt[D] + doubt[C] * doubt[D];
		cmABCD /= norm;

		double checkSum = cmA + cmB + cmC + cmD
						+ cmAB + cmAC + cmAD + cmBC + cmBD + cmCD
						+ cmABC + cmABD + cmACD + cmBCD
						+ cmABCD;
		*/
		double cmEmpty = m[A]    * m[B]    * m[C]    * m[D]
					   + mNOT[A] * mNOT[B] * mNOT[C] * mNOT[D];

		double norm = 1.0 - cmEmpty;
		//double norm = 1.0;

		double cmA = m[A] * mNOT[B]  * mNOT[C]  * mNOT[D]
				   + m[A] * mNOT[B]  * mNOT[C]  * doubt[D]
				   + m[A] * mNOT[B]  * doubt[C] * mNOT[D]
				   + m[A] * doubt[B] * mNOT[C]  * mNOT[D]
				   + m[A] * mNOT[B]  * doubt[C] * doubt[D]
				   + m[A] * doubt[B] * mNOT[C]  * doubt[D]
				   + m[A] * doubt[B] * doubt[C] * mNOT[D]
				   + m[A] * doubt[B] * doubt[C] * doubt[D]
				   + doubt[A] * mNOT[B] * mNOT[C] * mNOT[D];
		cmA /= norm;

		double cmB = m[B] * mNOT[A]  * mNOT[C]  * mNOT[D]
				   + m[B] * mNOT[A]  * mNOT[C]  * doubt[D]
				   + m[B] * mNOT[A]  * doubt[C] * mNOT[D]
				   + m[B] * doubt[A] * mNOT[C]  * mNOT[D]
				   + m[B] * mNOT[A]  * doubt[C] * doubt[D]
				   + m[B] * doubt[A] * mNOT[C]  * doubt[D]
				   + m[B] * doubt[A] * doubt[C] * mNOT[D]
				   + m[B] * doubt[A] * doubt[C] * doubt[D]
				   + doubt[B] * mNOT[A] * mNOT[C] * mNOT[D];
		cmB /= norm;

		double cmC = m[C] * mNOT[B]  * mNOT[A]  * mNOT[D]
				   + m[C] * mNOT[B]  * mNOT[A]  * doubt[D]
				   + m[C] * mNOT[B]  * doubt[A] * mNOT[D]
				   + m[C] * doubt[B] * mNOT[A]  * mNOT[D]
				   + m[C] * mNOT[B]  * doubt[A] * doubt[D]
				   + m[C] * doubt[B] * mNOT[A]  * doubt[D]
				   + m[C] * doubt[B] * doubt[A] * mNOT[D]
				   + m[C] * doubt[B] * doubt[A] * doubt[D]
				   + doubt[C] * mNOT[B] * mNOT[A] * mNOT[D];
		cmC /= norm;

		double cmD = m[D] * mNOT[B]  * mNOT[C]  * mNOT[A]
				   + m[D] * mNOT[B]  * mNOT[C]  * doubt[A]
				   + m[D] * mNOT[B]  * doubt[C] * mNOT[A]
				   + m[D] * doubt[B] * mNOT[C]  * mNOT[A]
				   + m[D] * mNOT[B]  * doubt[C] * doubt[A]
				   + m[D] * doubt[B] * mNOT[C]  * doubt[A]
				   + m[D] * doubt[B] * doubt[C] * mNOT[A]
				   + m[D] * doubt[B] * doubt[C] * doubt[A]
				   + doubt[D] * mNOT[B] * mNOT[C] * mNOT[A];
		cmD /= norm;


		double cmAB = (doubt[A] * doubt[B] * mNOT[C] * mNOT[B]) / norm;
		double cmAC = (doubt[A] * doubt[C] * mNOT[B] * mNOT[D]) / norm;
		double cmAD = (doubt[A] * doubt[D] * mNOT[B] * mNOT[C]) / norm;
		double cmBC = (doubt[B] * doubt[C] * mNOT[A] * mNOT[D]) / norm;
		double cmBD = (doubt[B] * doubt[D] * mNOT[A] * mNOT[C]) / norm;
		double cmCD = (doubt[C] * doubt[D] * mNOT[A] * mNOT[B]) / norm;

		double cmABC = (mNOT[D] * doubt[A] * doubt[B] * doubt[C]) / norm;
		double cmABD = (mNOT[C] * doubt[A] * doubt[B] * doubt[D]) / norm;
		double cmACD = (mNOT[B] * doubt[A] * doubt[C] * doubt[D]) / norm;
		double cmBCD = (mNOT[A] * doubt[B] * doubt[C] * doubt[D]) / norm;

		double cmABCD = (doubt[A] * doubt[B] * doubt[C] * doubt[D]) / norm;

		// Get belief and plausibilitiy:

		double [] belief = new double[]{cmA, cmB, cmC, cmD};
		double [] plaus = new double[]{cmBCD, cmACD, cmABD, cmABC};


		// Print results:
		int overallClass = -1;
		double topcConf = -1;
		System.out.println("\nClassification of protein " + prot.getName() + ":");
		for (int i = A; i <= D; i++) {
			double conf = belief[i] - plaus[i];
			System.out.println("    " + typeNames[i]
							 + "?\tBelief = " + belief[i]
							 + ";\tPlausibility = " + plaus[i] + ".");
			if (conf > topcConf) {
				overallClass = i;
				topcConf = conf;
			}
		}
		if (overallClass < 0)
			System.out.println("    DESISION: Cannot localise protein.");
		else
			System.out.println("    DESISION: (" + ((char) ('A' + overallClass)) + ") "
							   + typeNames[overallClass]
							   + " (confidence: " + topcConf
							   + " evience for: " + belief[overallClass]
							   + " evidence against: " + plaus[overallClass] + ")");
		return overallClass;
	}

	private boolean align(Protein protein, ProteinLocationClassifier classif) {

		final PolypeptidePattern pattern = classif.getPattern();
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

	private int classifyProtein(Protein prot) {
		if (null == classifiers || 0 == classifiers.length) {
			System.out.println("No classifiers available");
			return -1;
		}

		boolean [] match = new boolean[classifiers.length];
		for (int i = A; i <= D; i++) {
			match[i] = align(prot, classifiers[i]);
		}

		return analyseResults(prot, match);
	}

	public void exec(String fastaFile) {
		SimplifiedFastaFileParser parser = new SimplifiedFastaFileParser();
		ProteinGroup group = new ProteinGroup("Unknown", parser, fastaFile);
		for (int i = 0; i < group.size(); i++) {
			final Protein prot = group.getProtein(i);
			final int classification = classifyProtein(prot);
			if (classification > -1)
				totalClassifications[classification]++;
		}
		System.out.println("--------------------------------------------------------------------------------");
		System.out.println("TOTALS:");
		for (int i = A; i <= D; i++)
			System.out.println("Total " + typeNames[i] + ": " + totalClassifications[i]);
		System.out.println("TOTAL PROTEINS: " + group.size());

	}

	public static void main(String[] unusedArgs) {
		Locator locator = new Locator();
		locator.exec("D:/Courseworks/4C58/cw/data/Unk.fasta");
	}

}