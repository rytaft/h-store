package org.jaga.masterAlgorithm;

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

public class InitialPopulationGA extends ElitistGA {

	private Object [] initData = new Object[0];

	public InitialPopulationGA() {}

	public InitialPopulationGA(double eliteProportion) {
		super(eliteProportion);
	}

	public InitialPopulationGA(double eliteProportion, double badProportion) {
		super(eliteProportion, badProportion);
	}

	public InitialPopulationGA(GAParameterSet parameters, double eliteProportion) {
		super(parameters, eliteProportion);
	}

	public InitialPopulationGA(GAParameterSet parameters, double eliteProportion, double badProportion) {
		super(parameters, eliteProportion, badProportion);
	}

	public InitialPopulationGA(Object [] initData) {
		setInitialPopulation(initData);
	}

	public InitialPopulationGA(Object [] initData, double eliteProportion) {
		super(eliteProportion);
		setInitialPopulation(initData);
	}

	public InitialPopulationGA(Object [] initData, double eliteProportion, double badProportion) {
		super(eliteProportion, badProportion);
		setInitialPopulation(initData);
	}

	public InitialPopulationGA(Object [] initData, GAParameterSet parameters, double eliteProportion) {
		super(parameters, eliteProportion);
		setInitialPopulation(initData);
	}

	public InitialPopulationGA(Object [] initData, GAParameterSet parameters, double eliteProportion, double badProportion) {
		super(parameters, eliteProportion, badProportion);
		setInitialPopulation(initData);
	}

	public void setInitialPopulation(Object [] initData) {
		if (null == initData)
			throw new NullPointerException("initData may not be null");
		this.initData = initData;
	}

	protected Population createInitialPopulation(GAParameterSet params) {
		Population pop = createEmptyPopulation(params);

		for (int i = 0; i < params.getPopulationSize() && i < initData.length; i++) {
			Individual ind = params.getIndividualsFactory().createSpecificIndividual(initData[i], params);
			if (null != ind)
				pop.add(ind);
		}

		while (pop.getSize() < params.getPopulationSize()) {
			Individual ind;
			ind = params.getIndividualsFactory().createRandomIndividual(params);
			pop.add(ind);
		}
		return pop;
	}
}