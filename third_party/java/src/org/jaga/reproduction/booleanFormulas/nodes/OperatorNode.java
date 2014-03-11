package org.jaga.reproduction.booleanFormulas.nodes;


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

abstract public class OperatorNode extends BooleanFormulaTreeNode {

	BooleanFormulaTreeNode [] children = null;

	public OperatorNode() {}

	abstract public int getRequiredChildrenNumber();

	abstract String getOperatorName();

	public Object clone() {
		OperatorNode copy = null;
		try {
			copy = (OperatorNode) this.getClass().newInstance();
		} catch(InstantiationException e1) {
			throw new InstantiationError(e1.getMessage());
		} catch(IllegalAccessException e2) {
			throw new IllegalAccessError(e2.getMessage());
		}
		assert null != copy : "Problem creating a clone";

		if (null == this.children)
			return copy;

		for (int i = 0; i < children.length; i++) {
			if (null != children[i]) {
				copy.setChild(i, (BooleanFormulaTreeNode) children[i].clone());
			}
		}
		return copy;
	}

	public int recalcHeight() {

		int height = 0;
		for (int i = 0; i < getRequiredChildrenNumber(); i++) {
			BooleanFormulaTreeNode c = getChild(i);
			if (null != c) {
				int h = c.recalcHeight();
				if (h > height)
					height = h;
			}
		}
		++height;
		setHeight(height);

		OperatorNode parent = getParent();
		if (null != parent)
			parent.propagateNewHeight();

		return height;
	}

	void propagateNewHeight() {
		int height = 0;
		for (int i = 0; i < getRequiredChildrenNumber(); i++) {
			BooleanFormulaTreeNode c = getChild(i);
			if (null != c) {
				int h = c.getHeight();
				if (h > height)
					height = h;
			}
		}
		++height;
		setHeight(height);

		OperatorNode parent = getParent();
		if (null != parent)
			parent.propagateNewHeight();
	}

	public void setChild(int index, BooleanFormulaTreeNode node) {
		validateChildIndex(index);
		if (null == children)
			initChildrenArray();
		children[index] = node;
		node.setParent(this);
	}

	private void validateChildIndex(int index) throws IndexOutOfBoundsException {
		if (index < 0 || getRequiredChildrenNumber() <= index)
			throw new IndexOutOfBoundsException("Index is " + index
												+ ", but children-indices are from 0 to "
												+ getRequiredChildrenNumber());
	}

	public int findChild(BooleanFormulaTreeNode node) {
		for (int i = 0; i < getRequiredChildrenNumber(); i++)
			if (node == children[i])
				return i;
		return -1;
	}

	public BooleanFormulaTreeNode getChild(int index) {
		// validateChildIndex(index);  // this was generating 12.82% of run time
		if (null == children)
			return null;
		return children[index];
	}

	private void initChildrenArray() {
		if (null != children)
			return;
		children = new BooleanFormulaTreeNode[getRequiredChildrenNumber()];
		for (int i = 0; i < children.length; children[i++] = null);
	}

}