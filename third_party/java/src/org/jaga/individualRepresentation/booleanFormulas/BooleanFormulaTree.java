package org.jaga.individualRepresentation.booleanFormulas;

import org.jaga.reproduction.booleanFormulas.nodes.*;
import java.util.HashMap;
import java.util.Iterator;
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

public class BooleanFormulaTree implements Individual {

	// remember the max-depth constrain

	private int numberOfParameters = 0;
	private Fitness fitness = null;
	private HashMap nodes = new HashMap();
	private BooleanFormulaTreeNode root = null;
	private long nextHandleToGenerate = -Long.MIN_VALUE;


	private BooleanFormulaTree() {
		throw new java.lang.UnsupportedOperationException("Use the other constructor");
	}

	public BooleanFormulaTree(int numberOfParameters) {
		this.numberOfParameters = numberOfParameters;
		root = new FalseNode();
		addToNodeList(root, 0);
	}

	private Long generateNewHandle() {
		if (Long.MAX_VALUE == nextHandleToGenerate)
			throw new RuntimeException("nextHandleToGenerate="
									   + nextHandleToGenerate + ", which is too big; "
									   + " In this version, a formula tree cannot "
									   + "generate that much handles");
		Long handle = new Long(nextHandleToGenerate++);
		if (0 == nextHandleToGenerate)
			nextHandleToGenerate = 1;
		return handle;
	}

	public Fitness getFitness() {
		return this.fitness;
	}

	public void setFitness(Fitness fitness) {
		this.fitness = fitness;
	}

	public int getNumberOfParameters() {
		return this.numberOfParameters;
	}

	public Long selectRootNode() {
		return root.getHandle();
	}

	public int getNodeCount() {
		return this.nodes.size();
	}

	public Long selectRandomNode(GAParameterSet params) {
		int nodeCount = getNodeCount();
		if (0 == nodeCount)
			return new Long(0);
		int rndNum = params.getRandomGenerator().nextInt(0, nodeCount);
		Iterator key = getHandlersIterator();
		Object keyVal = key.next();
		int i = 0;
		while (i++ < rndNum)
			keyVal = key.next();
		return (Long) keyVal;
	}

	public Iterator getHandlersIterator() {
		return nodes.keySet().iterator();
	}

	public BooleanFormulaTreeNode exportNode(Long handle) {
		BooleanFormulaTreeNode node = getNode(handle);
		BooleanFormulaTreeNode clone = (BooleanFormulaTreeNode) node.clone();
		return clone;
	}

	public int getNodeDepth(Long handle) {
		BooleanFormulaTreeNode node = getNode(handle);
		return node.getDepth();
	}

	public int getNodeHeight(Long handle) {
		BooleanFormulaTreeNode node = getNode(handle);
		return node.getHeight();
	}

	private BooleanFormulaTreeNode getNode(Long handle) {
		if (0 == handle.longValue())
			throw new IllegalArgumentException("The handle 0 is illegal");
		BooleanFormulaTreeNode node = (BooleanFormulaTreeNode) nodes.get(handle);
		assert null != node : "Must be an invalid handle (" + handle.longValue() + ")";
		return node;
	}

	public void replaceNode(Long oldNodeHandle, BooleanFormulaTreeNode newNode) {

		if (null == newNode)
			throw new NullPointerException("Cannot replace a node by a null-pointer");

		BooleanFormulaTreeNode toReplace = getNode(oldNodeHandle);

		if (root == toReplace) {  // if we are replacing the root:
			nodes.clear();
			root = newNode;
			addToNodeList(newNode, 0);
			root.recalcHeight();

		} else {                   // any other node:
			removeFromNodeList(toReplace);
			OperatorNode parent = toReplace.getParent();
			assert null != parent : "Node has no parent!";
			int chInd = parent.findChild(toReplace);
			assert chInd >= 0 : "Need debug !!!!";
			parent.setChild(chInd, newNode);
			addToNodeList(newNode, parent.getDepth() + 1);
			parent.recalcHeight();
		}
	}

	public boolean evaluate(boolean [] parameters) {
		return root.evaluate(parameters);
	}

/*
	public boolean isIndividualValid(IndividualConstraint[] constraints) {
		for (int i = 0; i < constraints.length; i++) {
			if (!constraints[i].getApplicableClass().isInstance(this))
				throw new IllegalArgumentException("Constraint number " + i
												 + " can only be applied to "
												 + constraints[i].getApplicableClass().getName()
												 + " and does not support this class ("
												 + this.getClass().getName() + ")");
			if (!constraints[i].isIndividualLegal(this))
				return false;
		}
		return true;
	}
	*/

	public String toString() {
		return toString(true);
	}

	public String toString(boolean infix) {
		StringBuffer s = new StringBuffer("{formula=[");
		s.append(root.toStringBuffer(infix));
		s.append("], ");
		if (null == fitness) {
			s.append("fitness not known}");
		} else {
			s.append("fitness=");
			s.append(fitness.toString());
			s.append("}");
		}
		return s.toString();
	}

	private void removeFromNodeList(BooleanFormulaTreeNode node) {
		Long key = node.getHandle();
		nodes.remove(key);
		if (node instanceof OperatorNode) {
			OperatorNode parent = (OperatorNode) node;
			for (int i = 0; i < parent.getRequiredChildrenNumber(); i++) {
				BooleanFormulaTreeNode kid = parent.getChild(i);
				if (null != kid)
					removeFromNodeList(kid);
			}
		}
		node.setHandle(new Long(0));
	}

	private void addToNodeList(BooleanFormulaTreeNode node, int depth) {

		Long handle = null;

		node.setDepth(depth);
		handle = generateNewHandle();
		nodes.put(handle, node);
		node.setHandle(handle);

		if (node instanceof OperatorNode) {
			OperatorNode parent = (OperatorNode) node;
			for (int i = 0; i < parent.getRequiredChildrenNumber(); i++) {
				BooleanFormulaTreeNode kid = parent.getChild(i);
				if (null != kid)
					addToNodeList(kid, depth + 1);
			}
		}
	}

}