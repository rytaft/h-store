package org.jaga.individualRepresentation.booleanFormulas;

import org.jaga.reproduction.booleanFormulas.nodes.*;
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

public class BooleanFormulaTreeFactory implements IndividualsFactory {

	private static final Class [] stoppersWithConsts = new Class[] { TrueNode.class,
																	 FalseNode.class,
																	 TerminalNode.class };

	private static final Class [] stoppersWithoutConsts = new Class[] { TerminalNode.class };

	private int maxTreeDepth = 20;
	private int numberOfParameters = 6;
	private Class [] allowedNodeTypes = new Class[] { ANDNode.class, ORNode.class,
													  NOTNode.class, TerminalNode.class };
	private boolean allowConstants = true;

	public BooleanFormulaTreeFactory() {}

	public BooleanFormulaTreeFactory(int maxTreeDepth, int numberOfParameters,
									 Class [] allowedNodeTypes, boolean allowConstants) {
		this.numberOfParameters = numberOfParameters;
		if (null == allowedNodeTypes)
			this.allowedNodeTypes = new Class[0];
		else
			this.allowedNodeTypes = allowedNodeTypes;
		this.allowConstants = allowConstants;
		this.maxTreeDepth = maxTreeDepth;
	}

	public int getMaxTreeDepth() {
		return this.maxTreeDepth;
	}

	public void setMaxTreeDepth(int maxDepth) {
		if (maxDepth < 1)
			throw new IndexOutOfBoundsException("Max. tree depth may not be < 1");
		this.maxTreeDepth = maxDepth;
	}

	public int getNumberOfParameters() {
		return this.numberOfParameters;
	}

	public void setNumberOfParameters(int val) {
		if (numberOfParameters < 0)
			throw new IndexOutOfBoundsException("Number of parameters may not be < 0");
		this.numberOfParameters = val;
	}

	public Class [] getAllowedNodeTypes() {
		return this.allowedNodeTypes;
	}

	public void setAllowedNodeTypes(Class [] val) {
		if (null == allowedNodeTypes)
			this.allowedNodeTypes = new Class[0];
		else
			this.allowedNodeTypes = val;
	}

	public boolean getAllowConstants() {
		return this.allowConstants;
	}

	public void setAllowConstants(boolean val) {
		this.allowConstants = val;
	}

	public Individual createDefaultIndividual(GAParameterSet params) {
		BooleanFormulaTree formula = new BooleanFormulaTree(numberOfParameters);
		return formula;
	}

	public Individual createRandomIndividual(GAParameterSet params) {
		BooleanFormulaTree formula = null;
		Long root = null;
		BooleanFormulaTreeNode node = null;
		formula = new BooleanFormulaTree(numberOfParameters);
		root = formula.selectRootNode();
		node = createRandomNode(1, params);
		formula.replaceNode(root, node);
		return formula;
	}

	public Individual createSpecificIndividual(Object init, GAParameterSet params) {
		if (null == init)
			throw new NullPointerException("Cannot have an null initialisation object.");

		if (init instanceof BooleanFormulaTree)
			return clone((BooleanFormulaTree) init, params);

		throw new ClassCastException("Initialisation value for BooleanFormulaTree "
									 + "must be of type Integer (but is "
									 + init.getClass() + ")");
	}

	public BooleanFormulaTree clone(BooleanFormulaTree template, GAParameterSet params) {
		BooleanFormulaTree copy = new BooleanFormulaTree(template.getNumberOfParameters());
		copy.setFitness(template.getFitness());
		Long trh = template.selectRootNode();
		BooleanFormulaTreeNode nodeClone = template.exportNode(trh);
		Long crh = copy.selectRootNode();
		copy.replaceNode(crh, nodeClone);
		return copy;
	}

	public BooleanFormulaTreeNode createRandomNode(int thisDepth, GAParameterSet params) {

		BooleanFormulaTreeNode node = null;

		if (thisDepth >= this.maxTreeDepth)
			return createRandomStopNode(params);
		node = createOneNodeOfRandomType(allowedNodeTypes, params);

		for (int i = 0; i < node.getRequiredChildrenNumber(); i++) {
			BooleanFormulaTreeNode child = createRandomNode(thisDepth + 1, params);
			((OperatorNode) node).setChild(i, child);
		}

		return node;

	}

	private BooleanFormulaTreeNode createRandomStopNode(GAParameterSet params) {
		if (allowConstants)
			return createOneNodeOfRandomType(stoppersWithConsts, params);
		else
			return createOneNodeOfRandomType(stoppersWithoutConsts, params);
	}

	private BooleanFormulaTreeNode createOneNodeOfRandomType(Class [] nodeTypes, GAParameterSet params) {

		BooleanFormulaTreeNode node = null;

		int rndType = params.getRandomGenerator().nextInt(0, nodeTypes.length);
		Class type = nodeTypes[rndType];

		try {
			node = (BooleanFormulaTreeNode) type.newInstance();
		} catch (InstantiationException e1) {
			throw new Error("Dodgy: can't instantiate " + type.getName()
							+ ". (" + e1.getMessage() + ")");
		} catch (IllegalAccessException e2) {
			throw new Error("Dodgy: can't instantiate " + type.getName()
							+ ". (" + e2.getMessage() + ")");
		}

		assert null != node;

		if (node instanceof TerminalNode) {
			int rndTerm = params.getRandomGenerator().nextInt(0, this.numberOfParameters);
			((TerminalNode) node).setTerminalIndex(rndTerm);
		}

		return node;
	}

	/*
	//TEST:
	public static void main(String[] args) {
		GAParameterSet params = new com.gregPaperin.ga.simpleImplementation.DefaultParameterSet();
		BooleanFormulaTreeFactory fact = new BooleanFormulaTreeFactory();
		fact.setMaxTreeDepth(6);
		fact.setAllowConstants(false);
		for (int i = 0; i < 5; i++) {
			Individual form = fact.createRandomIndividual(params);
			System.out.println(i + ") " + form);
		}
	}
	*/

}