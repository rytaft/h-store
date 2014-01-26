package org.jaga.hooks;


import org.jaga.util.FittestIndividualResult;
import org.jaga.masterAlgorithm.SimpleGA;
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

public class DebugHook extends SimpleGAHook {

	public DebugHook() {
	}

	private void printPopulation(Population pop, int age) {
		System.out.println("Population in generation " + age + " has "
						   + pop.getSize() + " members:");
		for (int i = 0; i < pop.getSize(); i++) {
			System.out.println("  " + i + ") " + pop.getMember(i));
		}
	}

	private void printResult(GAResult result, int age) {
		if (!(result instanceof FittestIndividualResult)) {
			return;
		}
		System.out.println("Best result (in generation " + age + "):");
		System.out.println(((FittestIndividualResult) result).
						   getFittestIndividual());
	}

	protected void printIndividuals(Individual[] inds) {
		for (int i = 0; i < inds.length; i++) {
			System.out.println("  " + i + ": " + inds[i]);
		}
	}

	public void initialisationDone(SimpleGA caller, Population pop, int age,
								   GAResult result, GAParameterSet params) {
		System.out.println("\nINITIALISATION DONE.");
		printPopulation(pop, age);
		printResult(result, age);
		System.out.println("--------------------------------------------------");
	}

	public void foundNewResult(SimpleGA caller, Population pop, int age,
							   GAResult result, GAParameterSet params) {
		System.out.println("\nFOUND NEW RESULT.");
		printPopulation(pop, age);
		printResult(result, age);
		System.out.println("--------------------------------------------------");
	}

	public void generationChanged(SimpleGA caller, Population pop, int age,
								  GAResult result, GAParameterSet paramss) {
		System.out.println("\nNEXT GENERATION.");
		printPopulation(pop, age);
		printResult(result, age);
		System.out.println("--------------------------------------------------");
	}

	public void terminationConditionApplies(SimpleGA caller, Population pop,
											int age,
											GAResult result,
											GAParameterSet params) {
		System.out.println("\nTERMINATION APPLIED.");
		printPopulation(pop, age);
		printResult(result, age);
		System.out.println("--------------------------------------------------");
	}

	public void selectedForReproduction(SimpleGA caller,
										Individual[] selectedParents,
										Population pop, int age,
										GAResult result,
										GAParameterSet params) {
		System.out.println("\nPARENTS SELECTED.");
		printPopulation(pop, age);
		printResult(result, age);
		System.out.println("Parents:");
		printIndividuals(selectedParents);
		System.out.println("--------------------------------------------------");
	}

	public void reproduced(SimpleGA caller, Individual[] children,
						   Individual[] parents,
						   Population pop, int age, GAResult result,
						   GAParameterSet params) {
		System.out.println("\nCHILDREN PRODUCED.");
		printPopulation(pop, age);
		printResult(result, age);
		System.out.println("Parents:");
		printIndividuals(parents);
		System.out.println("Children:");
		printIndividuals(children);
		System.out.println("--------------------------------------------------");
	}

	public void fitnessCalculated(SimpleGA caller, Individual updatedIndividual,
								  Population pop, int age,
								  GAParameterSet params) {
		System.out.println("\nFITNESS CALCULATED.");
		System.out.println("Updated individual: " + updatedIndividual);
		System.out.println("--------------------------------------------------");
	}

}