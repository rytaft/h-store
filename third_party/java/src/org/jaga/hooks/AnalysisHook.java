package org.jaga.hooks;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import org.jaga.masterAlgorithm.SimpleGA;
import org.jaga.util.FittestIndividualResult;
import org.jaga.selection.AbsoluteFitness;
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

public class AnalysisHook extends SimpleGAHook {

	/**
	 * Statistical data entry.
	 */
	class Entry {
		private int generation = 0;
		private double value = 0;
		private Entry() {
			throw new UnsupportedOperationException("Use Entry(int gen, double val)");
		}
		private Entry(int gen, double val) {
			generation = gen;
			value = val;
		}
		public StringBuffer toStrBuf() {
			return (new StringBuffer()).append(value)
				  .append(" (gen. ").append(generation).append(")");
		}
		public int getGeneration() { return generation; }
		public double getValue() { return value; }
		public String toString() { return toStrBuf().toString(); }
	}

	// Control switches:
	private boolean analyseBestFitness = true;
	private boolean analyseTotalFitEvals = true;

	private boolean analyseGenAge = true;
	private boolean analyseGenMinFit = true;
	private boolean analyseGenMaxFit = true;
	private boolean analyseGenAverageFit = true;
	private boolean analyseGenFitStdDeviation = true;
	private boolean analyseGenDump = false;

	private boolean analyseRunTime = true;

	private PrintStream logStream = null;
	private boolean plotGraph = true;
	private long updateDelay = -1;

	// View rendering info:
	private GAAnalysisFrame frame = null;
	private boolean viewUpdated = false;
	private double minPlotVal = Double.MAX_VALUE;
	private double maxPlotVal = -Double.MAX_VALUE;
	private int maxPlotCount = Integer.MIN_VALUE;
	private String plotFrameTitle = "Genetic Algorithm Analysis";

	// Statistical data:
	private FittestIndividualResult bestResult = null;
	private ArrayList bestFitnessValues = new ArrayList();

	private int generationNum = -1;
	private ArrayList genAverageFitnesses = new ArrayList();
	private HashMap genFitnessStdDeviations = new HashMap();
	private ArrayList genMinFitnesses = new ArrayList();
	private ArrayList genMaxFitnesses = new ArrayList();

	private long fitnessCalculations = 0;
	private long startedTime = 0;
	private String runTime = "00:00.0";


	public AnalysisHook() {
		super();
		reset();
	}

	public AnalysisHook(PrintStream logStream) {
		super();
		this.logStream = logStream;
		reset();
	}

	public AnalysisHook(boolean plotGraph) {
		super();
		this.plotGraph = plotGraph;
		reset();
	}

	public AnalysisHook(PrintStream logStream, boolean plotGraph) {
		super();
		this.logStream = logStream;
		this.plotGraph = plotGraph;
		reset();
	}

	public void reset() {
		viewUpdated = false;
		minPlotVal = Double.MAX_VALUE;
		maxPlotVal = -Double.MAX_VALUE;
		maxPlotCount = Integer.MIN_VALUE;

		bestResult = null;
		bestFitnessValues.clear();

		generationNum = -1;
		genAverageFitnesses.clear();
		genFitnessStdDeviations.clear();
		genMinFitnesses.clear();
		genMaxFitnesses.clear();

		fitnessCalculations = 0;
		runTime = "00:00.0";

		if (isPlotGraph()) {
			if (null != frame)
				frame.dispose();
			frame = createAnalysisFrame();
			frame.show();
		}

		startedTime = System.currentTimeMillis();
	}

	private GAAnalysisFrame createAnalysisFrame() {
		GAAnalysisFrame frame = new GAAnalysisFrame(this);
		frame.pack();
		return frame;
	}

	public synchronized void frameDisposed() {
		frame = null;
	}

	private synchronized void updateResult(GAResult result, int age) {

		checkTime();

		if (null == result)
			return;

		FittestIndividualResult res = (FittestIndividualResult) result;
		AbsoluteFitness fit = (AbsoluteFitness) res.getBestFitness();

		if (null == fit)
			return;

		if (null != bestResult && !bestResult.getBestFitness().isWorse(fit))
			return;

		bestResult = new FittestIndividualResult();
		bestResult.setFittestIndividual(res.getFittestIndividual());

		if (!isAnalyseBestFitness())
			return;

		bestFitnessValues.add(new Entry(age, fit.getValue()));
		updateGraphSize(fit.getValue(), bestFitnessValues.size());
		log("\n");
		log(" ***  New best result in generation " + age + ":");
		log("      Individual: " + res.getFittestIndividual());
		log("      Fitness:    " + res.getBestFitness());
		updateView();
	}

	private synchronized void updatePopulation(Population pop, int age, GAResult result) {

		if (generationNum == age)
			return;

		generationNum = age;

		checkTime();

		int popSize = pop.getSize();
		double average = 0;
		double stddev = 0;
		double minfit = Double.MAX_VALUE;
		double maxfit = -Double.MAX_VALUE;

		if (isAnalyseGenMinFit() || isAnalyseGenMaxFit()
					|| isAnalyseGenAverageFit() || isAnalyseGenFitStdDeviation()) {
			// Av. fitness:
			for (int i = 0; i < popSize; i++) {
				Fitness f = pop.getMember(i).getFitness();
				AbsoluteFitness fit = (AbsoluteFitness) f;
				final double fval = fit.getValue();
				if (fval < minfit)  minfit = fval;
				if (fval > maxfit)  maxfit = fval;
				average += fval;
			}
			average /= (double) popSize;
			if (isAnalyseGenAverageFit() || isAnalyseGenFitStdDeviation()) {
				genAverageFitnesses.add(new Entry(age, average));
				updateGraphSize(average, genAverageFitnesses.size());
			}
			if (isAnalyseGenMinFit()) {
				genMinFitnesses.add(new Entry(age, minfit));
				updateGraphSize(minfit, genMinFitnesses.size());
			}
			if (isAnalyseGenMaxFit()) {
				genMaxFitnesses.add(new Entry(age, maxfit));
				updateGraphSize(maxfit, genMaxFitnesses.size());
			}

			if (isAnalyseGenFitStdDeviation()) {
				//Std. deviation:
				for (int i = 0; i < popSize; i++) {
					Fitness f = pop.getMember(i).getFitness();
					AbsoluteFitness fit = (AbsoluteFitness) f;
					final double d = fit.getValue() - average;
					stddev += d * d;
				}
				stddev /= (double) (popSize - 1);
				stddev = Math.sqrt(stddev);
				updateGraphSize(average + stddev, genAverageFitnesses.size());
				updateGraphSize(average - stddev, genAverageFitnesses.size());
				genFitnessStdDeviations.put(new Integer(age), new Entry(age, stddev));
			}
		}

		if (isAnalyseGenDump() || isAnalyseGenAge() || isAnalyseGenMinFit()
				|| isAnalyseGenMaxFit() || isAnalyseGenAverageFit()
				|| isAnalyseGenFitStdDeviation()) {
			log("\n");
		}
		if (isAnalyseGenDump()) {
			log("\n ***  List of generation " + age + " (" + popSize + " induviduals):");
			for (int i = 0; i < popSize; i++) {
				log("      " + i + ") " + pop.getMember(i));
			}
			log(" ***  List of the " + popSize + " individuals in generation " + age + " complete.");
		}
		if (isAnalyseGenAge()) {
			Runtime rt = Runtime.getRuntime();
			log(" ***  New generation " + age + " created (" + popSize + " individuals).");
			log("      Memory free: " + rt.freeMemory() + " KByte.");
			log("      Memory available: " + rt.totalMemory() + " KByte.");
			log("      Memory on system: " + rt.maxMemory() + " KByte.");
		}
		if (isAnalyseGenMinFit()) {
			log(" ***  Minimum fitness in generation " + age + " is " + minfit + ".");
		}
		if (isAnalyseGenMaxFit()) {
			log(" ***  Maximum fitness in generation " + age + " is " + maxfit + ".");
		}
		if (isAnalyseGenAverageFit()) {
			log(" ***  Average fitness in generation " + age + " is " + average + ".");
		}
		if (isAnalyseGenFitStdDeviation()) {
			log(" ***  Fitness standard deviation in generation " + age + " is " + stddev + ".");
		}
		if (isAnalyseGenAge() || isAnalyseGenAverageFit() || isAnalyseGenFitStdDeviation()) {
			updateView();
		}
	}

	private synchronized void gaTerminated() {

		log("\n ***  Computation completed.");
		log("      Generations: " + generationNum);
		log("      Fitness evaluations: " + fitnessCalculations);
		log("      Run time: " + getRunTimeString());
		log("      Best result: " + bestResult);

		if (isAnalyseBestFitness() && bestFitnessValues.size() > 0) {
			Entry e = (Entry) bestFitnessValues.get(bestFitnessValues.size() - 1);
			log(" ***  Best fitness (" + e.getValue()
				+ ") was discovered in generation " + e.getGeneration() + ".");
		}

		updateView();
	}

	private synchronized void fitnessCalculated() {
		++fitnessCalculations;
		if (!isAnalyseTotalFitEvals())
			return;
		updateView();
	}

	private void updateGraphSize(double val, int count) {
		if (val < minPlotVal)
			minPlotVal = val;
		if (val > maxPlotVal)
			maxPlotVal = val;
		if (count > maxPlotCount)
			maxPlotCount = count;
	}

	private void updateView() {
		if (viewUpdated)
			return;
		viewUpdated = true;
		if (updateDelay < 1 && null != frame)
			frame.updateView();
	}

	private void log(String msg) {
		if (null == logStream)
			return;
		logStream.println(msg);
	}

	private void checkTime() {
		if (!isAnalyseRunTime())
			return;
		runTime = getRunTimeString();
		log("\n ***  Run time: " + runTime);
		if (isAnalyseTotalFitEvals())
			log(" ***  Total fitness evaluations: " + fitnessCalculations);
		updateView();
	}

	private String getRunTimeString() {
		long millis = System.currentTimeMillis();
		millis -= startedTime;
		StringBuffer mins = new StringBuffer();
		mins.append(millis / 60000L);
		while (2 > mins.length())
			mins.insert(0, "0");
		StringBuffer secs = new StringBuffer();
		secs.append((millis % 60000L) / 1000L);
		while (2 > secs.length())
			secs.insert(0, "0");
		StringBuffer msecs = new StringBuffer();
		msecs.append(millis % 60000000L);
		return mins.append(":").append(secs).append(".").append(msecs).toString();
	}

	public void initialisationDone(SimpleGA caller, Population pop, int age,
								   GAResult result, GAParameterSet params) {
		updateResult(result, age);
		updatePopulation(pop, age, result);
	}

	public void populationReinitialised(Population pop, int age,
								   GAResult result, GAParameterSet params) {
		this.bestResult = null;
		updateResult(result, age);
		updatePopulation(pop, age, result);
	}

	public void foundNewResult(SimpleGA caller, Population pop, int age,
							   GAResult result, GAParameterSet params) {
		updateResult(result, age);
	}

	public void generationChanged(SimpleGA caller, Population pop, int age,
								  GAResult result, GAParameterSet paramss) {
		updatePopulation(pop, age, result);
	}

	public void terminationConditionApplies(SimpleGA caller, Population pop, int age,
											GAResult result, GAParameterSet params) {
		gaTerminated();
	}

	public void selectedForReproduction(SimpleGA caller, Individual [] selectedParents,
										Population pop, int age, GAResult result,
										GAParameterSet params) {
		;
	}

	public void reproduced(SimpleGA caller, Individual [] children, Individual [] parents,
						   Population pop, int age, GAResult result, GAParameterSet params) {
		;
	}

	public void fitnessCalculated(SimpleGA caller, Individual updatedIndividual,
								  Population pop, int age, GAParameterSet params) {
		fitnessCalculated();
	}

	public boolean isAnalyseBestFitness() {
		return analyseBestFitness;
	}

	public void setAnalyseBestFitness(boolean analyseBestFitness) {
		this.analyseBestFitness = analyseBestFitness;
	}



	public boolean isAnalyseGenAge() {
		return analyseGenAge;
	}

	public void setAnalyseGenAge(boolean analyseGenAge) {
		this.analyseGenAge = analyseGenAge;
	}

	public boolean isAnalyseGenAverageFit() {
		return analyseGenAverageFit;
	}

	public void setAnalyseGenAverageFit(boolean analyseGenAverageFit) {
		this.analyseGenAverageFit = analyseGenAverageFit;
	}

	public boolean isAnalyseGenFitStdDeviation() {
		return analyseGenFitStdDeviation;
	}

	public void setAnalyseGenFitStdDeviation(boolean analyseGenFitStdDeviation) {
		this.analyseGenFitStdDeviation = analyseGenFitStdDeviation;
	}

	public boolean isAnalyseGenDump() {
		return analyseGenDump;
	}

	public void setAnalyseGenDump(boolean analyseGenDump) {
		this.analyseGenDump = analyseGenDump;
	}

	public boolean isAnalyseRunTime() {
		return analyseRunTime;
	}

	public void setAnalyseRunTime(boolean analyseRunTime) {
		this.analyseRunTime = analyseRunTime;
	}

	public boolean isAnalyseTotalFitEvals() {
		return analyseTotalFitEvals;
	}

	public void setAnalyseTotalFitEvals(boolean analyseTotalFitEvals) {
		this.analyseTotalFitEvals = analyseTotalFitEvals;
	}

	public PrintStream getLogStream() {
		return logStream;
	}

	public void setLogStream(PrintStream logStream) {
		this.logStream = logStream;
	}

	public void stopLogStream() {
		this.logStream = null;
	}

	public boolean isAnalyseGenMaxFit() {
		return analyseGenMaxFit;
	}

	public void setAnalyseGenMaxFit(boolean analyseGenMaxFit) {
		this.analyseGenMaxFit = analyseGenMaxFit;
	}

	public boolean isAnalyseGenMinFit() {
		return analyseGenMinFit;
	}

	public void setAnalyseGenMinFit(boolean analyseGenMinFit) {
		this.analyseGenMinFit = analyseGenMinFit;
	}

	public boolean isPlotGraph() {
		return plotGraph;
	}

	public void setPlotGraph(boolean plotGraph) {
		this.plotGraph = plotGraph;
	}

	public String getPlotFrameTitle() {
		return plotFrameTitle;
	}

	public void setPlotFrameTitle(String plotFrameTitle) {
		this.plotFrameTitle = plotFrameTitle;
	}

	public long getUpdateDelay() {
		return updateDelay;
	}

	public void setUpdateDelay(long updateDelay) {
		this.updateDelay = updateDelay;
	}

	public synchronized boolean isViewUpdated() {
		return viewUpdated;
	}

	public synchronized int getGenerationNum() {
		return generationNum;
	}

	public synchronized long getFitnessCalculations() {
		return fitnessCalculations;
	}

	public synchronized String getRunTime() {
		return runTime;
	}

	public synchronized Entry getBestFitnessValue(int index) {
		if (index < 0 || bestFitnessValues.size() <= index)
			return new Entry(-1, 0);
		return (Entry) bestFitnessValues.get(index);
	}

	public synchronized int getBestFitnessValueCount() {
		return bestFitnessValues.size();
	}

	public synchronized Entry getMinFitness(int index) {
		if (index < 0 || genMinFitnesses.size() <= index)
			return new Entry(-1, 0);
		return (Entry) genMinFitnesses.get(index);
	}

	public synchronized int getMinFitnessCount() {
		return genMinFitnesses.size();
	}

	public synchronized Entry getMaxFitness(int index) {
		if (index < 0 || genMaxFitnesses.size() <= index)
			return new Entry(-1, 0);
		return (Entry) genMaxFitnesses.get(index);
	}

	public synchronized int getMaxFitnessCount() {
		return genMaxFitnesses.size();
	}

	public synchronized Entry getAverageFitness(int index) {
		if (index < 0 || genAverageFitnesses.size() <= index)
			return new Entry(-1, 0);
		return (Entry) genAverageFitnesses.get(index);
	}

	public synchronized int getAverageFitnessCount() {
		return genAverageFitnesses.size();
	}

	public synchronized int getFitnessStdDeviationCount() {
		return genFitnessStdDeviations.size();
	}

	public synchronized double getMaxPlotVal() {
		return maxPlotVal;
	}

	public synchronized double getMinPlotVal() {
		return minPlotVal;
	}

	public synchronized int getMaxPlotCount() {
		return maxPlotCount;
	}

	public synchronized void viewUpdateComplete() {
		viewUpdated = false;
	}

	public ArrayList getBestFitnessValues() {
		return bestFitnessValues;
	}

	public ArrayList getGenAverageFitnesses() {
		return genAverageFitnesses;
	}

	public ArrayList getGenMaxFitnesses() {
		return genMaxFitnesses;
	}

	public ArrayList getGenMinFitnesses() {
		return genMinFitnesses;
	}

	public FittestIndividualResult getBestResult() {
		return bestResult;
	}

	public Entry getStdDeviation(int genNum) {
		Integer key = new Integer(genNum);
		Object o = genFitnessStdDeviations.get(key);
		if (null == key)
			return new Entry(-1, 0);
		else
			return (Entry) o;
	}
}