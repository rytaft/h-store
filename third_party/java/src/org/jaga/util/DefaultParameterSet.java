package org.jaga.util;

import java.util.*;

import org.jaga.selection.RouletteWheelSelection;
import org.jaga.fitnessEvaluation.largerNumbers.LargerDecimals;
import org.jaga.individualRepresentation.greycodedNumbers.NDecimalsIndividualSimpleFactory;
import org.jaga.reproduction.greycodedNumbers.SimpleBinaryXOverWithMutation;

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

public class DefaultParameterSet implements GAParameterSet {

	private HashMap params = null;

	public DefaultParameterSet() {
		setDefaultvalues();
	}

	public String toString() {
		StringBuffer s = new StringBuffer(
				 "\n individualsFactory:         ");
		s.append(params.get("individualsFactory").getClass().getName());

		s.append("\n populationSize:             ");
		s.append(((Integer) params.get("populationSize")).intValue());

		s.append("\n maxGenerationNumber:        ");
		s.append(((Integer) params.get("maxGenerationNumber")).intValue());

		s.append("\n reproductionAlgorithm:      ");
		s.append(params.get("reproductionAlgorithm").getClass().getName());

		s.append("\n selectionAlgorithm:         ");
		s.append(params.get("selectionAlgorithm").getClass().getName());

		s.append("\n fitnessEvaluationAlgorithm: ");
		s.append(params.get("fitnessEvaluationAlgorithm").getClass().getName());

		s.append("\n maxBadReproductionAttempts: ");
		s.append(((Integer) params.get("maxBadReproductionAttempts")).intValue());

		s.append("\n randomGenerator:            ");
		s.append(params.get("randomGenerator").getClass().getName());

		s.append("\n useMainAlgorithmHooks:      ");
		s.append(((Boolean) params.get("useMainAlgorithmHooks")).booleanValue());

		s.append("\n}");
		return s.toString();
	}

	public void setDefaultvalues() {
		params = new HashMap();

		IndividualsFactory indFac = new NDecimalsIndividualSimpleFactory();
		params.put("individualsFactory", indFac);

		Integer popSize = new Integer(100);
		params.put("populationSize", popSize);

		Integer maxPop = new Integer(500);
		params.put("maxGenerationNumber", maxPop);

		ReproductionAlgorithm reprAlg = new SimpleBinaryXOverWithMutation(0.65, 0.05);
		params.put("reproductionAlgorithm", reprAlg);

		SelectionAlgorithm selAlg = new RouletteWheelSelection();
		params.put("selectionAlgorithm", selAlg);

		FitnessEvaluationAlgorithm fitAlg = new LargerDecimals();
		params.put("fitnessEvaluationAlgorithm", fitAlg);

		Integer mAtt = new Integer(15);
		params.put("maxBadReproductionAttempts", mAtt);

		RandomGenerator rnd = new DefaultRandomGenerator(System.currentTimeMillis());
		params.put("randomGenerator", rnd);

		Boolean useHook = new Boolean(true);
		params.put("useMainAlgorithmHooks", useHook);
	}

	protected Object fetchParameter(String paramName) {
		Object val = params.get(paramName);
		if (null == val)
			throw new IllegalStateException("Trying to fetch a non-existing parameter \""
											+ paramName + "\"");
		return val;
	}

	public IndividualsFactory getIndividualsFactory() {
		Object val = fetchParameter("individualsFactory");
		return (IndividualsFactory) val;
	}

	public void setIndividualsFactory(IndividualsFactory val) {
		if (null == val)
			throw new NullPointerException("GA paremeter value may not be null");
		params.put("individualsFactory", val);
	}

	public int getPopulationSize() {
		Object val = fetchParameter("populationSize");
		return ((Integer) val).intValue();
	}

	public void setPopulationSize(int val) {
		if (0 > val)
			throw new NullPointerException("GA paremeter value may not be negative");
		Integer popSize = new Integer(val);
		params.put("populationSize", popSize);
	}

	public int getMaxGenerationNumber() {
		Object val = fetchParameter("maxGenerationNumber");
		return ((Integer) val).intValue();
	}

	public void setMaxGenerationNumber(int val) {
		if (0 > val)
			throw new NullPointerException("GA paremeter value may not be negative");
		Integer maxPop = new Integer(val);
		params.put("maxGenerationNumber", maxPop);
	}

	public ReproductionAlgorithm getReproductionAlgorithm() {
		Object val = fetchParameter("reproductionAlgorithm");
		return (ReproductionAlgorithm) val;
	}

	public void setReproductionAlgorithm(ReproductionAlgorithm val) {
		if (null == val)
			throw new NullPointerException("GA paremeter value may not be null");
		params.put("reproductionAlgorithm", val);
	}

	public SelectionAlgorithm getSelectionAlgorithm() {
		Object val = fetchParameter("selectionAlgorithm");
		return (SelectionAlgorithm) val;
	}

	public void setSelectionAlgorithm(SelectionAlgorithm val) {
		if (null == val)
			throw new NullPointerException("GA paremeter value may not be null");
		params.put("selectionAlgorithm", val);
	}

	public FitnessEvaluationAlgorithm getFitnessEvaluationAlgorithm() {
		Object val = fetchParameter("fitnessEvaluationAlgorithm");
		return (FitnessEvaluationAlgorithm) val;
	}

	public void setFitnessEvaluationAlgorithm(FitnessEvaluationAlgorithm val) {
		if (null == val)
			throw new NullPointerException("GA paremeter value may not be null");
		params.put("fitnessEvaluationAlgorithm", val);
	}

	public int getMaxBadReproductionAttempts() {
		Object val = fetchParameter("maxBadReproductionAttempts");
		return ((Integer) val).intValue();
	}

	public void setMaxBadReproductionAttempts(int val) {
		if (0 > val)
			throw new NullPointerException("GA paremeter value may not be negative");
		Integer mAtt = new Integer(val);
		params.put("maxBadReproductionAttempts", mAtt);
	}

	public RandomGenerator getRandomGenerator() {
		Object val = fetchParameter("randomGenerator");
		return (RandomGenerator) val;
	}

	public void setRandomGenerator(RandomGenerator val) {
		if (null == val)
			throw new NullPointerException("GA paremeter value may not be null");
		params.put("randomGenerator", val);
	}

	public boolean getUseMainAlgorithmHooks() {
		Object val = fetchParameter("useMainAlgorithmHooks");
		return ((Boolean) val).booleanValue();
	}

	public void setUseMainAlgorithmHooks(boolean val) {
		Boolean useHook = new Boolean(val);
		params.put("useMainAlgorithmHooks", useHook);
	}
}