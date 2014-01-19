package org.jaga.exampleApplications.proteinLocation;

import org.jaga.individualRepresentation.proteinLocation.*;
import org.jaga.definitions.*;
import org.jaga.fitnessEvaluation.proteinLocation.*;
import org.jaga.util.*;
import org.jaga.masterAlgorithm.*;
import org.jaga.reproduction.*;
import org.jaga.hooks.*;
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

public class Validator {

	public Validator() {}

	public void exec() {

		GAParameterSet params = new DefaultParameterSet();

		SimplifiedFastaFileParser parser = new SimplifiedFastaFileParser();
		ProteinGroup cytosol = new ProteinGroup("Cytosol", parser, "D:/Courseworks/4C58/cw/data/Cytosol.valid.dat");
		ProteinGroup extracellular = new ProteinGroup("Extracellular", parser, "D:/Courseworks/4C58/cw/data/Extracellular.valid.dat");
		ProteinGroup nucleus = new ProteinGroup("Nucleus", parser, "D:/Courseworks/4C58/cw/data/Nucleus.valid.dat");
		ProteinGroup mitochondrion = new ProteinGroup("Mitochondrion", parser, "D:/Courseworks/4C58/cw/data/Mitochondrion.valid.dat");

		params.setMaxGenerationNumber(1);
		CombinedReproductionAlgorithm repAlg = new CombinedReproductionAlgorithm();
		params.setReproductionAlgorithm(repAlg);
		params.setSelectionAlgorithm(new TwoTournamentProbabalisticSelection(1.0));
		ProteinLocationClassifierFactory factory = new ProteinLocationClassifierFactory();
		params.setIndividualsFactory(factory);
		AnalysisHook analysisHook = new AnalysisHook();
		analysisHook.setLogStream(System.out);
		analysisHook.setUpdateDelay(1500);

		System.out.println("\n\n  **  **  ******************** CYTOSOL: ********************  **  **");

		ProteinGroup allPositives = new ProteinGroup();
		allPositives.add(cytosol);
		ProteinGroup allNegatives = new ProteinGroup();
		allNegatives.add(mitochondrion);
		allNegatives.add(extracellular);
		allNegatives.add(nucleus);
		params.setFitnessEvaluationAlgorithm(new ProteinLocationTrainer(allPositives, allNegatives, 3));
		factory.setClassifierName("Cytosol");

		Object [] classif = new Object [] {
			"<Hydrophobic>-<Hydrophobic>-<Small>-[M]-[SAD]-<Polar>-<Polar>-<Aliphatic>-[KCGTP]-[NTQKR]-[FVCTHW]-[FCTHM]-<Small>-[VWFAMHDRSNQT]-[VWFAMHDRSNTE]-<Polar>-[ETHS]-[YETLH]-[LFH]-[DQPFLHTY]-[KGCISRW]-<Small>-<Hydrophobic>-<Polar>-<Small>-<Tiny>-<Tiny>-<Small>-[DGCPL]-[PCVSNR]-[CDVAKH]-[AQ]-[CWTI]-<Aliphatic>-<Negative>-<Small>-<Small>-<Small>-<Small>-<Small>-[HVFQSM]-[IRKEWQ]-[HNYDMG]-<Positive>-[LTKVI]-[DEF]-<Hydrophobic>-[RPLSECTVMFD]-[CDVNKH]-[CDVNKH]-[PCTV]-[CTAR]-<Polar>-<Polar>-<Small>-[CVPS]-[GTAVM]-[PNDVCL]-[DWHEC]-<Polar>-<Polar>-<Polar>-[STPNC]-[KDH]-[YWDENM]-[YWTLK]-<Small>-<Small>-[RPLYSECTVMI]-[ATVPL]-?-?-<Small>-?-<Hydrophobic>-?-[RTNCYMKISFPE]-[EDHW]-[EALKVNDH]-[MWEFKTVNGRCAYIP]-?",
			"<Hydrophobic>-[DTRWSN]-<Polar>-<Hydrophobic>-<Small>-[VTCP]-[DA]-<Positive>-<Positive>-[SAG]-[DKPLS]-[NQEV]-<Small>-[NGCPT]-[DEF]-<Polar>-[ILVDPA]-[RHG]-[DQYM]-<Small>-<Hydrophobic>-[GQL]-<Hydrophobic>-[HQYMEW]-[HQYME]-<Hydrophobic>-[ASNFK]-<Polar>-<Hydrophobic>-[NQSLV]-<Hydrophobic>-<Hydrophobic>-<Tiny>-[YHGTP]-<Polar>-<Polar>-<Small>-<Aliphatic>-<Small>-<Small>-<Hydrophobic>-<Small>-<Tiny>-[YGTMC]-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-[IMN]-<Hydrophobic>-[SEC]-<Aromatic>-<Aromatic>-<Aromatic>-[KAFTIG]-<Small>-<Tiny>-[WSKDV]-<Polar>-<Hydrophobic>-[MLAQD]-<Aromatic>-W-[YWFHA]-<Small>-<Small>-[CILVGS]-[REDKG]-[HYCT]-<Tiny>-<Hydrophobic>-[STNYC]-<Negative>-<Negative>-<Hydrophobic>-[VLP]-[YHTD]-[EKM]-[PAY]-E-<Hydrophobic>-<Aliphatic>-<Aromatic>-<Aromatic>-[MKGWTL]-[ILVFKN]-[IWKTA]-[AIMYC]-<Hydrophobic>-<Hydrophobic>-[SCAMDFNRP]-[MAGTEHCP]-[MANYEFPHDT]-[ASDTGC]-[GSQICRVWDLE]-<Polar>-[SPGFQWD]-[SPVTALC]-?-?"
		};
		params.setPopulationSize(2);
		InitialPopulationGA ga = new InitialPopulationGA(classif, params, 1.0, 0.0);
		ga.addHook(analysisHook);

		FittestIndividualResult result = (FittestIndividualResult) ((ReusableSimpleGA) ga).exec();
		ProteinLocationClassifier cytosolClass = (ProteinLocationClassifier) result.getFittestIndividual();


		System.out.println("\n\n  **  **  ******************** NUCLEUS: ********************  **  **");

		allPositives = new ProteinGroup();
		allPositives.add(nucleus);
		allNegatives = new ProteinGroup();
		allNegatives.add(mitochondrion);
		allNegatives.add(extracellular);
		allNegatives.add(cytosol);
		params.setFitnessEvaluationAlgorithm(new ProteinLocationTrainer(allPositives, allNegatives, 3));
		factory.setClassifierName("Nucleus");

		classif = new Object [] {
			"[IVLN]-[GFVM]-[HEF]-<Hydrophobic>-[RKHT]-[WAV]-[VILP]-[NRQIWKDM]-[HWAKG]-<Small>-<Hydrophobic>-[GP]-[RQLNS]-[ADNGE]-<Small>-<Aliphatic>-[GSARE]-<Polar>-[CNTSF]-<Small>-<Aromatic>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-R-[WNQDH]-[YHTG]-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Small>-[NDLI]-[NIVWKD]-<Hydrophobic>-[GYVHTK]-[WLHMTR]-<Polar>-<Small>-<Aliphatic>-<Hydrophobic>-<Polar>-<Negative>-[ADEIL]-<Hydrophobic>-<Hydrophobic>-[MKGTY]-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Aliphatic>-<Hydrophobic>-[EDC]-<Polar>-[NQCMVDTE]-[NQKRMVDIY]-[CSGKH]-<Small>-<Aliphatic>-[EDI]-[NVGPCY]-<Polar>-<Aliphatic>-[ADTGQ]-<Aromatic>-<Aliphatic>-<Hydrophobic>-[DPVY]-<Aromatic>-<Aromatic>-[KTYVIF]-<Hydrophobic>-<Aromatic>-[FHYWRS]-<Small>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-[CADRMEL]-[PDMEGIHNT]-?-?-?-?-[NGDKAEVYRCFQ]-?-?-?-?-?-[TNDMKVAFSPGRCI]-[TCMLSERWIDAFGPVKQN]-?-[RLFNPKDEQHGSYTC]-?-[SILDMGQRYPTFNEHW]-[MIQSRPNFGDVHELW]-[ATVGHQPW]-[EDM]",
			"[LHWVY]-<Hydrophobic>-<Aromatic>-[DWKQVL]-[NPDHE]-[RLE]-<Aromatic>-<Hydrophobic>-[PTVG]-<Hydrophobic>-[PCNGE]-[CEYRVQ]-<Hydrophobic>-<Hydrophobic>-<Small>-<Polar>-[QH]-[RNKQ]-[EYNKS]-[REKD]-<Hydrophobic>-[LWYGFM]-[ACMLHK]-<Negative>-[RTQ]-<Small>-<Small>-<Aromatic>-<Small>-<Aromatic>-[NGFP]-<Hydrophobic>-<Small>-<Small>-[KHN]-<Small>-[GLRMEW]-<Positive>-<Polar>-<Small>-[SGADC]-<Small>-<Small>-<Polar>-[CASW]-<Aliphatic>-[FCN]-[FCN]-[GPVA]-[ED]-[LGYMF]-[HFE]-<Small>-[WLMYQ]-P-<Hydrophobic>-[MWFHG]-[IVD]-<Aliphatic>-<Tiny>-[VITQ]-[FGCS]-<Small>-<Small>-[CSDAG]-<Small>-<Small>-<Polar>-<Hydrophobic>-<Small>-<Small>-<Aromatic>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-[GASY]-[LYW]-<Polar>-[AYGW]-[EQKMDLC]-<Positive>-<Small>-<Small>-<Polar>-[DCPM]-<Aliphatic>-<Aliphatic>-<Positive>-[ACWL]-<Hydrophobic>-<Hydrophobic>-[GAMLSPVN]-[LGYMNPQKFSHV]-[MLNPQGVYRS]-<Small>-[MGDKPVQSA]-<Polar>-[PSDRMTCQ]-[VMHPSYNGFD]",
			"[NSL]-<Aromatic>-[ACGTR]-<Small>-<Negative>-<Negative>-[IVLTHE]-<Polar>-<Tiny>-<Negative>-<Negative>-D-<Aromatic>-<Hydrophobic>-<Negative>-[LFAYERWD]-[LAYEC]-[GASH]-<Small>-<Polar>-<Hydrophobic>-[EMV]-[IC]-<Small>-[EDRV]-<Positive>-[FYW]-<Positive>-[QEKYD]-<Small>-<Polar>-[YQWTGV]-<Small>-[FGMLS]-[LWGAHT]-<Small>-<Small>-<Small>-<Aliphatic>-<Small>-<Small>-<Hydrophobic>-<Hydrophobic>-<Negative>-<Hydrophobic>-[KWQGNYLAEPS]-<Hydrophobic>-?-?-?-[MPISKYGWRNAFD]-?-?-?-?-?-?-?-[RNSHCWGEMPI]-?-?-?-?-?-[QAESPNR]-?-<Hydrophobic>-?-?-?-?-?-?-?-?-?-?-?-?-?-?-?-?-[TMIRVNFSHG]-?-?-?-[AMSRIQFVNGEDT]-[MAPYNTFSRIDHWV]-<Hydrophobic>-?-[SADGHLIMCKFNQRV]-[EGKLCMIHATW]-[MSATDPLKGYNEFCR]-[MSRNGAKQEWPHC]-[NMSGDTQVYKRW]-[NKLSADTYERPVFHW]-[NSKVPQDEY]-[NMSEGPKTCY]-[DERQYNHPM]"
		};
		params.setPopulationSize(3);
		ga = new InitialPopulationGA(classif, params, 1.0, 0.0);
		analysisHook.reset();
		ga.addHook(analysisHook);

		result = (FittestIndividualResult) ((ReusableSimpleGA) ga).exec();
		ProteinLocationClassifier nucleusClass = (ProteinLocationClassifier) result.getFittestIndividual();


		System.out.println("\n\n  **  **  ******************** MITOCHONDTION: ********************  **  **");

		allPositives = new ProteinGroup();
		allPositives.add(mitochondrion);
		allNegatives = new ProteinGroup();
		allNegatives.add(nucleus);
		allNegatives.add(extracellular);
		allNegatives.add(cytosol);
		params.setFitnessEvaluationAlgorithm(new ProteinLocationTrainer(allPositives, allNegatives, 3));
		factory.setClassifierName("Mitochondrion");

		classif = new Object [] {
			"<Aliphatic>-<Small>-<Small>-[MHWT]-[QRDPL]-[WQRDP]-[TSCPK]-[TNAGD]-[TGPCI]-[KWYES]-[FYHW]-[FYHW]-[WY]-<Aromatic>-<Aromatic>-[MP]-<Tiny>-[GPDCTK]-<Tiny>-<Hydrophobic>-[RYVELC]-<Small>-<Small>-<Aromatic>-[YNHK]-[YNHK]-[DEVL]-[ILS]-<Small>-[SI]-[WYHA]-<Aliphatic>-<Positive>-<Small>-[SAPTH]-<Hydrophobic>-<Hydrophobic>-[TREYH]-[RKI]-[RKI]-[DQI]-[HKE]-[PHD]-<Small>-<Small>-<Hydrophobic>-<Polar>-[SNCDK]-<Aliphatic>-[LISWY]-<Tiny>-<Aliphatic>-<Small>-[KHRQ]-<Tiny>-[HCVGK]-<Hydrophobic>-<Aliphatic>-[KGFDL]-[VIP]-[NWQLEISGA]-<Tiny>-<Tiny>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-[CNGVAM]-[LEQ]-<Positive>-<Hydrophobic>-<Hydrophobic>-[RHKWC]-[WKHS]-[AMWLT]-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-[WLAYF]-[LGCYSE]-[MHTIWN]-[CTPNY]-<Tiny>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Small>-<Small>-<Hydrophobic>-[VKEFWLA]-<Hydrophobic>-<Hydrophobic>-[WEARNLDG]-<Hydrophobic>-[GLMRINDASQPY]-[LMREFINVAGT]-[FMLIYW]-[LRFI]-[SAGRPFWQKNITH]-[TRSCAKMYPV]"
		};
		params.setPopulationSize(1);
		ga = new InitialPopulationGA(classif, params, 1.0, 0.0);
		analysisHook.reset();
		ga.addHook(analysisHook);

		result = (FittestIndividualResult) ((ReusableSimpleGA) ga).exec();
		ProteinLocationClassifier mitochondrionClass = (ProteinLocationClassifier) result.getFittestIndividual();

		System.out.println("\n\n  **  **  ******************** EXTRACELLULAR: ********************  **  **");

		allPositives = new ProteinGroup();
		allPositives.add(extracellular);
		allNegatives = new ProteinGroup();
		allNegatives.add(mitochondrion);
		allNegatives.add(nucleus);
		allNegatives.add(cytosol);
		params.setFitnessEvaluationAlgorithm(new ProteinLocationTrainer(allPositives, allNegatives, 3));
		factory.setClassifierName("Extracellular");

		classif = new Object [] {
			"[QWYTE]-[KLWF]-[MWGVP]-[PLT]-[PLT]-[ASGE]-<Negative>-<Hydrophobic>-[WICVDQ]-<Hydrophobic>-<Small>-<Small>-<Hydrophobic>-<Tiny>-[YSEN]-<Hydrophobic>-[LHMYID]-[LHMYID]-<Small>-<Small>-[VSTAW]-<Positive>-<Hydrophobic>-<Aromatic>-<Aromatic>-<Polar>-[WLTIMR]-[EKC]-<Small>-A-<Hydrophobic>-<Small>-<Small>-[RAL]-<Hydrophobic>-<Polar>-[LVIT]-[ASGTH]-[NKDYWF]-[WSTQ]-[WQHIPN]-[ESDW]-<Hydrophobic>-<Hydrophobic>-[LVFCHK]-[CYKAT]-<Hydrophobic>-[QPWYKR]-<Small>-[GSAV]-<Hydrophobic>-[ST]-[TCPA]-[VPDC]-[DNW]-[DNL]-[YEWTD]-G-<Small>-[TCL]-[HRW]-[EGV]-[DVP]-<Small>-<Small>-<Small>-[CHYR]-<Small>-[FYHW]-<Small>-[SQNYWL]-[KTDQE]-<Positive>-<Positive>-<Hydrophobic>-<Hydrophobic>-<Hydrophobic>-<Small>-<Small>-<Small>-<Hydrophobic>-[AWYIE]-[SGWN]-[YMVTSH]-[PTCNWGQDVHMI]-?-?-?-[MKRPSHWATFVLEQC]-?-?-?-?-?-?-[FLINSQVWC]-<Hydrophobic>-[LCFW]-[LI]-<Hydrophobic>"
		};
		params.setPopulationSize(1);
		ga = new InitialPopulationGA(classif, params, 1.0, 0.0);
		analysisHook.reset();
		ga.addHook(analysisHook);

		result = (FittestIndividualResult) ((ReusableSimpleGA) ga).exec();
		ProteinLocationClassifier extracellularClass = (ProteinLocationClassifier) result.getFittestIndividual();


		Locator locator = new Locator();
		locator.setClassifiers(cytosolClass, nucleusClass,
							   mitochondrionClass, extracellularClass);
		locator.exec("D:/Courseworks/4C58/cw/data/Unk.fasta");
		//locator.exec("D:/Courseworks/4C58/cw/data/Cytosol.valid.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Nucleus.valid.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Mitochondrion.valid.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Extracellular.valid.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Cytosol.train.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Nucleus.train.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Mitochondrion.train.dat");
		//locator.exec("D:/Courseworks/4C58/cw/data/Extracellular.train.dat");

	}

	public static void main(String[] unusedArgs) {
		Validator validator = new Validator();
		validator.exec();
	}

}