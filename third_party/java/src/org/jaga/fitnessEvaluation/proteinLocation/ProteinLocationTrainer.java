package org.jaga.fitnessEvaluation.proteinLocation;

import org.jaga.individualRepresentation.proteinLocation.*;
import org.jaga.definitions.*;
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

public class ProteinLocationTrainer implements FitnessEvaluationAlgorithm {

	private ProteinGroup positiveGroup = null;
	private ProteinGroup negativeGroup = null;
	private int minOverlap = 3;

	private ProteinLocationClassifier best = null;

	private ProteinLocationTrainer() {}

	public ProteinLocationTrainer(ProteinGroup positiveGroup, ProteinGroup negativeGroup, int minOverlap) {
		this.positiveGroup = positiveGroup;
		this.negativeGroup = negativeGroup;
		this.minOverlap = minOverlap;
	}

	public Class getApplicableClass() {
		return ProteinLocationClassifier.class;
	}

	public void setPositiveGroup(ProteinGroup pGroup) {
		if (null == pGroup)
			throw new NullPointerException("Group cannot be null.");
		this.positiveGroup = pGroup;
	}

	public ProteinGroup getPositiveGroup() {
		return this.positiveGroup;
	}

	public void setNegativeGroup(ProteinGroup nGroup) {
		if (null == nGroup)
			throw new NullPointerException("Group cannot be null.");
		this.negativeGroup = nGroup;
	}

	public ProteinGroup getNegativeGroup() {
		return this.negativeGroup;
	}

	public int getMinOverlap() {
		return minOverlap;
	}

	public void setMinOverlap(int minOverlap) {
		this.minOverlap = minOverlap;
	}

	public Fitness evaluateFitness(Individual individual, int age, Population population, GAParameterSet params) {
		ProteinLocationClassifier classifier = (ProteinLocationClassifier) individual;
		PolypeptidePattern pattern = classifier.getPattern();

		int truePositives = positiveGroup.align(pattern, minOverlap);
		int falsePositives = negativeGroup.align(pattern, minOverlap);
		int falseNegatives = positiveGroup.size() - truePositives;
		int trueNegatives = negativeGroup.size() - falsePositives;

		double sensitivity, specificity, fitVal;

		// Sensitivity (% of positives recognised correctly):
		if (0 == truePositives + falseNegatives)
			sensitivity = 1.;
		else
			sensitivity = ((double) truePositives) / ((double) (truePositives + falseNegatives));

		// Specificity (in medical sence, i.e. % of negatives recognised correctly):
		if (0 == trueNegatives + falsePositives)
			specificity = 1.;
		else
			specificity = ((double) trueNegatives) / ((double) (trueNegatives + falsePositives));

/*
		if (10 > age) {
			best = (ProteinLocationClassifier) individual;
			fitVal = sensitivity + specificity;
			Fitness fit = new ClassifierFitness(truePositives, trueNegatives,
									 falsePositives, falseNegatives,
									 sensitivity, specificity,
									 fitVal);
			return fit;

		}

		ClassifierFitness bfit = (ClassifierFitness) best.getFitness();
		double r = bfit.getSensitivity() / bfit.getSpecificity();
		if (0 == r)
			r = 1;
		fitVal = sensitivity + specificity * r;
		if (0 == sensitivity || 0 == specificity)
			fitVal = 0;
		Fitness fit = new ClassifierFitness(truePositives, trueNegatives,
									 falsePositives, falseNegatives,
									 sensitivity, specificity,
									 fitVal);
		if (fit.isBetter(bfit)) {
			best = (ProteinLocationClassifier) individual;
		}
		return fit;
*/
/*
return new ClassifierFitness(truePositives, trueNegatives,
									 falsePositives, falseNegatives,
									 sensitivity, specificity,
									 sensitivity + specificity);
									 */

	  double prod = (double) (truePositives + trueNegatives)
					* (double) (trueNegatives + falsePositives)
					* (double) (truePositives + falseNegatives)
					* (double) (truePositives + falsePositives);
	  double denom = Math.sqrt(prod);
	  double mcc = 0;
	  if (denom != 0)
		  mcc = (truePositives * trueNegatives - falseNegatives * falsePositives)
				/ denom;

	  return new ClassifierFitness(truePositives, trueNegatives,
									 falsePositives, falseNegatives,
									 sensitivity, specificity,
									 mcc);

	}
}