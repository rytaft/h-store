package org.jaga.reproduction.proteinLocation;

import org.jaga.reproduction.Mutation;
import org.jaga.individualRepresentation.proteinLocation.*;
import java.util.ArrayList;
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

public class PolypeptidePatternMutation extends Mutation {

	private SubstitutionScoringMatrix scoringMatrix = new Blosum62();

	private int aminoAcid_deleteChance = 2;
	private int aminoAcid_cloneChance = 2;
	private int aminoAcid_mutateChance = 5;
	private int aminoAcid_generaliseToPropertyChance = 3;
	private int aminoAcid_generaliseToGroupChance = 2;

	private int group_substractChance = 5;
	private int group_addChance = 5;
	private int group_swapChance = 9;
	private int group_deleteChance = 2;
	private int group_cloneChance = 2;
	private int group_onEmptyDeleteChance = 1;
	private int group_onEmptyReplaceChance = 1;
	private double group_onSingleReplaceProb = 0.75;
	private double group_onPropertyTriggerPercent = 0.7;
	private double group_onPropertyReplaceProb = 0.5;
	private int group_onLargeTriggerLength = 12;
	private int group_onLargeSplitChance = 2;
	private int group_onLargeReplaceChance = 3;
	private int group_onLargeDontActChance = 5;

	private int property_deleteChance = 2;
	private int property_cloneChance = 5;
	private int property_mutateChance = 5;
	private int property_expandChance = 5;//3
	private int property_expandGroupSize = 6;
	private int property_expandSamePropCount = 5;

	private int singleGap_deleteChance = 3;
	private int singleGap_cloneChance = 5;
	private int singleGap_mutateChance = 5;
	private int singleGap_expandChance = 2;
	private int singleGap_expandGroupSize = 20;


	public PolypeptidePatternMutation() {
		super();
	}

	public PolypeptidePatternMutation(double mutProb) {
		super(mutProb);
	}

	public Class getApplicableClass() {
		return ProteinLocationClassifier.class;
	}

	private PolypeptidePatternItem [] mutateItem(AminoAcid aAcid, GAParameterSet params) {
		int sum = aminoAcid_cloneChance + aminoAcid_deleteChance + aminoAcid_mutateChance
					+ aminoAcid_generaliseToPropertyChance + aminoAcid_generaliseToGroupChance;
		int dice = params.getRandomGenerator().nextInt(0, sum);

		// clone:
		if (0 <= dice && dice < aminoAcid_cloneChance) {
			return new PolypeptidePatternItem [] {aAcid, aAcid};

		// delete:
		} else if (aminoAcid_cloneChance <= dice && dice < aminoAcid_cloneChance + aminoAcid_deleteChance) {
			return new PolypeptidePatternItem[0];

		// mutate:
		} else if (aminoAcid_cloneChance + aminoAcid_deleteChance <= dice
				   && dice < aminoAcid_cloneChance + aminoAcid_deleteChance + aminoAcid_mutateChance) {
			/*
			AminoAcid substitute = scoringMatrix.chooseProbabalisticMutation(aAcid, params);
			if (null == substitute)
				return new PolypeptidePatternItem[] {SingleGap.getInstance()};
			else
				return new PolypeptidePatternItem[] {substitute};
			*/
			return new PolypeptidePatternItem[] {AminoAcidFactory.getRandomResidue(params)};

		// generalise to a property group:
		} else if (aminoAcid_cloneChance + aminoAcid_deleteChance + aminoAcid_mutateChance <= dice
				   && dice < aminoAcid_cloneChance + aminoAcid_deleteChance
												   + aminoAcid_mutateChance
												   + aminoAcid_generaliseToPropertyChance) {
			ArrayList props = new ArrayList();
			if (aAcid.isSmall())        props.add(AminoAcidPropertyFactory.Small);
			if (aAcid.isHydrophobic())  props.add(AminoAcidPropertyFactory.Hydrophobic);
			if (aAcid.isPolar())        props.add(AminoAcidPropertyFactory.Polar);
			if (aAcid.isPositive())     props.add(AminoAcidPropertyFactory.Positive);
			if (aAcid.isNegative())     props.add(AminoAcidPropertyFactory.Negative);
			if (aAcid.isTiny())         props.add(AminoAcidPropertyFactory.Tiny);
			if (aAcid.isAliphatic())    props.add(AminoAcidPropertyFactory.Aliphatic);
			if (aAcid.isAromatic())     props.add(AminoAcidPropertyFactory.Aromatic);
			Object p = props.get(params.getRandomGenerator().nextInt(0, props.size()));
			return new PolypeptidePatternItem [] {(AminoAcidProperty) p};

		// must be generalise to list group:
		} else {
			AminoAcidGroup group = new AminoAcidGroup();
			group.addResidue(aAcid);
			return new PolypeptidePatternItem [] {group};

		}
	}

	private PolypeptidePatternItem [] mutateItem(AminoAcidGroup group, GAParameterSet params) {
		/*
		   - delete an item from the group
		   - add an item to the group
		   - swap an item in the group
		   - delete the whole group
		   - clone the group
		   then check in this order:
		   - if group becomes empty, it wont be able to match anything, so:
			   - delete it or
			   - replace by a gap
		   - if group has only one residue - consider replacing group by that acid
		   - if more then X percent in group have same property - consider replacing by property group
		   - if group becomes too large, consider:
			   - splitting in two groups
			   - replacing by a gap
		 */
		RandomGenerator rnd = params.getRandomGenerator();
		int sum = group_substractChance + group_addChance + group_swapChance
				  + group_deleteChance + group_cloneChance;
		int dice = rnd.nextInt(0, sum);

		// clone:
		if (0 <= dice && dice < group_cloneChance) {
			return new PolypeptidePatternItem[] {group, group};

		// delete:
		} else if (group_cloneChance <= dice && dice < group_cloneChance + group_deleteChance) {
			return new PolypeptidePatternItem[0];

		// substract a residue:
		} else if (group_cloneChance + group_deleteChance <= dice
				   && dice < group_cloneChance + group_deleteChance + group_substractChance) {
			int ind = rnd.nextInt(0, group.size());
			group.removeResidue(ind);

		// add a residue:
		} else if (group_cloneChance + group_deleteChance + group_substractChance <= dice
				   && dice < group_cloneChance + group_deleteChance + group_substractChance + group_addChance) {
			group.addResidue(AminoAcidFactory.getRandomResidue(params));

		// must be swap a residue:
		} else {
			int ind = rnd.nextInt(0, group.size());
			group.swapResidue(ind, AminoAcidFactory.getRandomResidue(params));

		}

		// Now the checks:

		if (group.size() == 0) {
			dice = rnd.nextInt(0, group_onEmptyDeleteChance + group_onEmptyReplaceChance);
			if (dice < group_onEmptyDeleteChance)
				return new PolypeptidePatternItem[0];
			else
				return new PolypeptidePatternItem [] {SingleGap.getInstance()};
		}
		if (group.size() == 1) {
			if (rnd.nextDouble() < group_onSingleReplaceProb)
				return new PolypeptidePatternItem [] {group.getResidue(0)};
		}
		int frequentProp = group.getMaxRepresentedProperty();
		if (0 != frequentProp) {
			if (group.getPropertyProportion(frequentProp) >= group_onPropertyTriggerPercent)
				if (rnd.nextDouble() < group_onPropertyReplaceProb)
					return new PolypeptidePatternItem [] {AminoAcidPropertyFactory.getPropertyByCode(frequentProp)};
		}
		if (group.size() >= group_onLargeTriggerLength) {
			dice = rnd.nextInt(0, group_onLargeSplitChance
								+ group_onLargeReplaceChance + group_onLargeDontActChance);
			if (dice < group_onLargeSplitChance) {
				AminoAcidGroup group2 = new AminoAcidGroup();
				int half = group.size() / 2;
				for (int i = 0; i < half; i++) {
					int ind = rnd.nextInt(0, group.size());
					AminoAcid aa = group.removeResidue(ind);
					group2.addResidue(aa);
				}
				return new PolypeptidePatternItem [] {group, group2};

			} else if (dice < group_onLargeSplitChance + group_onLargeReplaceChance) {
				return new PolypeptidePatternItem [] {SingleGap.getInstance()};

			} else ; // Don't act = do nothing.
		}

		return new PolypeptidePatternItem [] {group};
	}

	private PolypeptidePatternItem [] mutateItem(AminoAcidProperty property, GAParameterSet params) {
		/*
		- delete the hole group
		- duplicate the group
		- mutate to some other property
		- mutate to aa group consisting of at least X% of acids with this property
		*/
		int sum = property_deleteChance + property_cloneChance
							   + property_mutateChance + property_expandChance;
		int dice = params.getRandomGenerator().nextInt(0, sum);

		// clone:
		if (0 <= dice && dice < property_cloneChance) {
			return new PolypeptidePatternItem [] {property, property};

		// delete:
		} else if (property_cloneChance <= dice && dice < property_cloneChance + property_deleteChance) {
			return new PolypeptidePatternItem[0];

		// mutate:
		} else if (property_cloneChance + property_deleteChance <= dice
				   && dice < property_cloneChance + property_deleteChance + property_mutateChance) {
			return new PolypeptidePatternItem[] {AminoAcidPropertyFactory.getRandomProperty(params)};

		// must be expand:
		} else {
			AminoAcidGroup group = new AminoAcidGroup();
			for (int i = 0; i < property_expandGroupSize; i++) {
				AminoAcid aa;
				if (i < property_expandSamePropCount)
					aa = AminoAcidFactory.getRandomResidueByProperty(property.getProperty(), params);
				else
					aa = AminoAcidFactory.getRandomResidue(params);
				group.addResidue(aa);
			}
			return new PolypeptidePatternItem [] {group};

		}
	}

	private PolypeptidePatternItem [] mutateItem(SingleGap gap, GAParameterSet params) {
		int sum = singleGap_cloneChance + singleGap_deleteChance
				  + singleGap_mutateChance + singleGap_expandChance;
		int dice = params.getRandomGenerator().nextInt(0, sum);

		// clone:
		if (0 <= dice && dice < singleGap_cloneChance) {
			return new PolypeptidePatternItem [] {gap, gap};

		// delete:
		} else if (singleGap_cloneChance <= dice && dice < singleGap_cloneChance + singleGap_deleteChance) {
			return new PolypeptidePatternItem[0];

		// mutate:
		} else if (singleGap_cloneChance + singleGap_deleteChance <= dice
				   && dice < singleGap_cloneChance + singleGap_deleteChance + singleGap_mutateChance) {
			AminoAcid substitute = scoringMatrix.chooseProbabalisticMutation((AminoAcid) null, params);
			if (null == substitute)
				return new PolypeptidePatternItem[] {SingleGap.getInstance()};
			else
				return new PolypeptidePatternItem[] {substitute};

		// must be expand:
		} else {
			AminoAcidGroup group = new AminoAcidGroup();
			for (int i = 0; i < singleGap_expandGroupSize; i++) {
				AminoAcid aa = AminoAcidFactory.getRandomResidue(params);
				group.addResidue(aa);
			}
			return new PolypeptidePatternItem [] {group};

		}
	}

	private PolypeptidePatternItem [] mutatePatternItem(PolypeptidePatternItem item, GAParameterSet params) {

		if (item instanceof AminoAcid)
			return mutateItem((AminoAcid) item, params);
		if (item instanceof AminoAcidGroup)
			return mutateItem((AminoAcidGroup) item, params);
		if (item instanceof AminoAcidProperty)
			return mutateItem((AminoAcidProperty) item, params);
		if (item instanceof SingleGap)
			return mutateItem((SingleGap) item, params);
		throw new IllegalArgumentException("Cannot mutate a PolypeptidePatternItem"
										   + " of an unexpected type ("
										   + item.getClass().getName() + ")");
	}

	public Individual reproduce(Individual parent, GAParameterSet params) {
		ProteinLocationClassifierFactory factory = (ProteinLocationClassifierFactory) params.getIndividualsFactory();
		ProteinLocationClassifier kid = (ProteinLocationClassifier)
										factory.createSpecificIndividual(parent, params);
		PolypeptidePattern pattern = kid.getPattern();
		RandomGenerator rnd = params.getRandomGenerator();
		double mutProb = getMutationProbability();
		int i = 0;
		while(i < pattern.getLength()) {
			if (rnd.nextDouble() > mutProb) {
				i++;
				continue;
			}
			PolypeptidePatternItem item = kid.getPattern().getItem(i);
			PolypeptidePatternItem [] res = mutatePatternItem(item, params);
			if (0 == res.length) {
				pattern.removeItem(i);
				i++;
			} else if (1 == res.length) {

				//if (null == res[0]) System.out.println("\n**********" + item.getClass().getName());

				pattern.replaceItem(i, res[0]);
				i++;
			} else { /* must be 1 < res.length */
				pattern.replaceItem(i, res[0]);
				i++;
				for (int j = 1; j < res.length; i++, j++) {
					if (pattern.getLength() >= factory.getMaxPatternLength())
						break;
					pattern.insertItem(i, res[j]);
				}
			}
		}
		return kid;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {
		Individual [] kids = new Individual[parents.length];
		for (int i = 0; i < parents.length; i++) {
			kids[i] = reproduce(parents[i], params);
		}
		return kids;
	}

	public int getAminoAcid_cloneChance() {
		return aminoAcid_cloneChance;
	}

	public void setAminoAcid_cloneChance(int aminoAcid_cloneChance) {
		this.aminoAcid_cloneChance = aminoAcid_cloneChance;
	}

	public int getAminoAcid_deleteChance() {
		return aminoAcid_deleteChance;
	}

	public void setAminoAcid_deleteChance(int aminoAcid_deleteChance) {
		this.aminoAcid_deleteChance = aminoAcid_deleteChance;
	}

	public int getAminoAcid_generaliseToGroupChance() {
		return aminoAcid_generaliseToGroupChance;
	}

	public void setAminoAcid_generaliseToGroupChance(int
		  aminoAcid_generaliseToGroupChance) {
		this.aminoAcid_generaliseToGroupChance =
			  aminoAcid_generaliseToGroupChance;
	}

	public int getAminoAcid_generaliseToPropertyChance() {
		return aminoAcid_generaliseToPropertyChance;
	}

	public void setAminoAcid_generaliseToPropertyChance(int
		  aminoAcid_generaliseToPropertyChance) {
		this.aminoAcid_generaliseToPropertyChance =
			  aminoAcid_generaliseToPropertyChance;
	}

	public int getAminoAcid_mutateChance() {
		return aminoAcid_mutateChance;
	}

	public void setAminoAcid_mutateChance(int aminoAcid_mutateChance) {
		this.aminoAcid_mutateChance = aminoAcid_mutateChance;
	}

	public int getGroup_addChance() {
		return group_addChance;
	}

	public void setGroup_addChance(int group_addChance) {
		this.group_addChance = group_addChance;
	}

	public int getGroup_cloneChance() {
		return group_cloneChance;
	}

	public void setGroup_cloneChance(int group_cloneChance) {
		this.group_cloneChance = group_cloneChance;
	}

	public int getGroup_deleteChance() {
		return group_deleteChance;
	}

	public void setGroup_deleteChance(int group_deleteChance) {
		this.group_deleteChance = group_deleteChance;
	}

	public int getGroup_onEmptyDeleteChance() {
		return group_onEmptyDeleteChance;
	}

	public void setGroup_onEmptyDeleteChance(int group_onEmptyDeleteChance) {
		this.group_onEmptyDeleteChance = group_onEmptyDeleteChance;
	}

	public int getGroup_onEmptyReplaceChance() {
		return group_onEmptyReplaceChance;
	}

	public void setGroup_onEmptyReplaceChance(int group_onEmptyReplaceChance) {
		this.group_onEmptyReplaceChance = group_onEmptyReplaceChance;
	}

	public int getGroup_onLargeDontActChance() {
		return group_onLargeDontActChance;
	}

	public void setGroup_onLargeDontActChance(int group_onLargeDontActChance) {
		this.group_onLargeDontActChance = group_onLargeDontActChance;
	}

	public int getGroup_onLargeReplaceChance() {
		return group_onLargeReplaceChance;
	}

	public void setGroup_onLargeReplaceChance(int group_onLargeReplaceChance) {
		this.group_onLargeReplaceChance = group_onLargeReplaceChance;
	}

	public int getGroup_onLargeSplitChance() {
		return group_onLargeSplitChance;
	}

	public void setGroup_onLargeSplitChance(int group_onLargeSplitChance) {
		this.group_onLargeSplitChance = group_onLargeSplitChance;
	}

	public int getGroup_onLargeTriggerLength() {
		return group_onLargeTriggerLength;
	}

	public void setGroup_onLargeTriggerLength(int group_onLargeTriggerLength) {
		this.group_onLargeTriggerLength = group_onLargeTriggerLength;
	}

	public double getGroup_onPropertyReplaceProb() {
		return group_onPropertyReplaceProb;
	}

	public void setGroup_onPropertyReplaceProb(double
											   group_onPropertyReplaceProb) {
		this.group_onPropertyReplaceProb = group_onPropertyReplaceProb;
	}

	public double getGroup_onPropertyTriggerPercent() {
		return group_onPropertyTriggerPercent;
	}

	public void setGroup_onPropertyTriggerPercent(double
		  group_onPropertyTriggerPercent) {
		this.group_onPropertyTriggerPercent = group_onPropertyTriggerPercent;
	}

	public double getGroup_onSingleReplaceProb() {
		return group_onSingleReplaceProb;
	}

	public void setGroup_onSingleReplaceProb(double group_onSingleReplaceProb) {
		this.group_onSingleReplaceProb = group_onSingleReplaceProb;
	}

	public int getGroup_substractChance() {
		return group_substractChance;
	}

	public void setGroup_substractChance(int group_substractChance) {
		this.group_substractChance = group_substractChance;
	}

	public int getGroup_swapChance() {
		return group_swapChance;
	}

	public void setGroup_swapChance(int group_swapChance) {
		this.group_swapChance = group_swapChance;
	}

	public int getProperty_cloneChance() {
		return property_cloneChance;
	}

	public void setProperty_cloneChance(int property_cloneChance) {
		this.property_cloneChance = property_cloneChance;
	}

	public int getProperty_deleteChance() {
		return property_deleteChance;
	}

	public void setProperty_deleteChance(int property_deleteChance) {
		this.property_deleteChance = property_deleteChance;
	}

	public int getProperty_expandChance() {
		return property_expandChance;
	}

	public void setProperty_expandChance(int property_expandChance) {
		this.property_expandChance = property_expandChance;
	}

	public int getProperty_expandGroupSize() {
		return property_expandGroupSize;
	}

	public void setProperty_expandGroupSize(int property_expandGroupSize) {
		this.property_expandGroupSize = property_expandGroupSize;
	}

	public int getProperty_expandSamePropCount() {
		return property_expandSamePropCount;
	}

	public void setProperty_expandSamePropCount(int
												property_expandSamePropCount) {
		this.property_expandSamePropCount = property_expandSamePropCount;
	}

	public int getProperty_mutateChance() {
		return property_mutateChance;
	}

	public void setProperty_mutateChance(int property_mutateChance) {
		this.property_mutateChance = property_mutateChance;
	}

	public SubstitutionScoringMatrix getScoringMatrix() {
		return scoringMatrix;
	}

	public void setScoringMatrix(SubstitutionScoringMatrix scoringMatrix) {
		this.scoringMatrix = scoringMatrix;
	}

	public int getSingleGap_cloneChance() {
		return singleGap_cloneChance;
	}

	public void setSingleGap_cloneChance(int singleGap_cloneChance) {
		this.singleGap_cloneChance = singleGap_cloneChance;
	}

	public int getSingleGap_deleteChance() {
		return singleGap_deleteChance;
	}

	public void setSingleGap_deleteChance(int singleGap_deleteChance) {
		this.singleGap_deleteChance = singleGap_deleteChance;
	}

	public int getSingleGap_expandChance() {
		return singleGap_expandChance;
	}

	public void setSingleGap_expandChance(int singleGap_expandChance) {
		this.singleGap_expandChance = singleGap_expandChance;
	}

	public int getSingleGap_expandGroupSize() {
		return singleGap_expandGroupSize;
	}

	public void setSingleGap_expandGroupSize(int singleGap_expandGroupSize) {
		this.singleGap_expandGroupSize = singleGap_expandGroupSize;
	}

	public int getSingleGap_mutateChance() {
		return singleGap_mutateChance;
	}

	public void setSingleGap_mutateChance(int singleGap_mutateChance) {
		this.singleGap_mutateChance = singleGap_mutateChance;
	}

}