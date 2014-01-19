package org.jaga.fitnessEvaluation.multiplexer;


import org.jaga.individualRepresentation.booleanFormulas.*;
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

public class MultiplexerFitness implements FitnessEvaluationAlgorithm {

	private class TestEntry {
		private boolean [] testVals = null;
		private boolean refResult = false;
		private TestEntry() {assert false : "Dont use!";}
		public TestEntry(Multiplexer refMulti, int encodedVals) {
			testVals = new boolean[refMulti.getTotalLines()];
			for (int i = refMulti.getTotalLines() - 1; i >= 0; i--) {
				testVals[i] = (0 != (encodedVals & 0x1));
				encodedVals >>>= 1;
			}
			refResult = refMulti.evaluate(testVals);
		}
		public boolean [] getTestVals() {
			return testVals;
		}
		public boolean getReferenceResult() {
			return refResult;
		}
		public String toString() {
			StringBuffer s = new StringBuffer("[");
			for (int i = 0; i < testVals.length; i++) {
				if (i > 0)
					s.append(", ");
				s.append(testVals[i] ? "1" : "0");
			}
			s.append("] => ");
			s.append(refResult ? "1" : "0");
			return s.toString();
		}
	}

	private TestEntry [] tests = null;
	private double pressForShortResultFactor = 0.0;

	public MultiplexerFitness() {
		initTests(new Multiplexer());
	}

	public MultiplexerFitness(Multiplexer referenceMultiplexer) {
		if (null == referenceMultiplexer)
			throw new NullPointerException("referenceMultiplexer may not be null");
		initTests(referenceMultiplexer);
	}

	public MultiplexerFitness(double pressForShortFactor) {
		this.pressForShortResultFactor = pressForShortFactor;
		initTests(new Multiplexer());
	}

	public MultiplexerFitness(Multiplexer referenceMultiplexer, double pressForShortFactor) {
		if (null == referenceMultiplexer)
			throw new NullPointerException("referenceMultiplexer may not be null");
		this.pressForShortResultFactor = pressForShortFactor;
		initTests(referenceMultiplexer);
	}

	public Class getApplicableClass() {
		return BooleanFormulaTree.class;
	}

	public double getPressForShortFactor() {
		return this.pressForShortResultFactor;
	}

	public void setPressForShortFactor(double pressForShortFactor) {
		this.pressForShortResultFactor = pressForShortFactor;
	}

	private void initTests(Multiplexer referenceMultiplexer) {
		int testCount = (int) Math.round(Math.pow(2, referenceMultiplexer.getTotalLines()));
		tests = new TestEntry[testCount];
		for (int encodedTstVals = 0; encodedTstVals < testCount; encodedTstVals++) {
			tests[encodedTstVals] = new TestEntry(referenceMultiplexer, encodedTstVals);
		}
	}

	public Fitness evaluateFitness(Individual individual, int age, Population population, GAParameterSet params) {
		BooleanFormulaTree formula = (BooleanFormulaTree) individual;
		int correct = 0;
		for (int i = 0; i < tests.length; i++) {
			boolean expected = tests[i].getReferenceResult();
			boolean [] args = tests[i].getTestVals();
			boolean actual = formula.evaluate(args);
			if (expected == actual)
				++correct;
		}
		double fract = ((double) correct) / ((double) tests.length);
		double whole = (double) correct;

		// double fitVal = whole + fract;

		double fitVal = Math.round(whole -
					pressForShortResultFactor * Math.log(formula.getNodeCount()) / Math.log(2)
						 ) + fract;

		AbsoluteFitness fitness = new AbsoluteFitness(fitVal);
		return fitness;
	}

	/*
	//TEST:
	public static void main(String[] args) {
		Multiplexer mult = new Multiplexer(4);
		MutliplexorFitness fitEval = new MutliplexorFitness(mult);
		for (int t = 0; t < fitEval.tests.length; ++t)
			System.out.println(fitEval.tests[t]);
	}
	*/

}