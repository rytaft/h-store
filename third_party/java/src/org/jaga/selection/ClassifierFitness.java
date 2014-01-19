package org.jaga.selection;


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

public class ClassifierFitness extends AbsoluteFitness {

	private int truePositives = 0;
	private int trueNegatives = 0;
	private int falsePositives = 0;
	private int falseNegatives = 0;
	private double sensitivity = 0.;
	private double specificity = 0.;

	private ClassifierFitness() {
		super(Double.NEGATIVE_INFINITY);
	};

	public ClassifierFitness(int truePositive, int trueNegative,
							 int falsePositive, int falseNegative,
							 double sensitivity, double specificity,
							 double fitnessValue) {
		super(fitnessValue);
		this.truePositives = truePositive;
		this.trueNegatives = trueNegative;
		this.falsePositives = falsePositive;
		this.falseNegatives = falseNegative;
		this.sensitivity = sensitivity;
		this.specificity = specificity;
	}

	public String toString() {
		StringBuffer s = new StringBuffer();
		s.append(getValue());
		s.append(" (TP=");
		s.append(this.truePositives);
		s.append(", TN=");
		s.append(this.trueNegatives);
		s.append(", FP=");
		s.append(this.falsePositives);
		s.append(", FN=");
		s.append(this.falseNegatives);
		s.append(", Sens=");
		s.append(this.sensitivity);
		s.append(", Spec=");
		s.append(this.specificity);
		s.append(", Quality=");
		s.append(this.sensitivity + this.specificity);
		s.append(")");
		return s.toString();
	}

	public int getFalseNegatives() {
		return falseNegatives;
	}

	public int getFalsePositives() {
		return falsePositives;
	}

	public int getTrueNegatives() {
		return trueNegatives;
	}

	public int getTruePositives() {
		return truePositives;
	}

	/**
	 * in medical sence
	 */
	public double getSensitivity() {
		return sensitivity;
	}

	/**
	 * in medical sence
	 */
	public double getSpecificity() {
		return specificity;
	}
}