package org.jaga.exampleApplications.proteinLocation;

import java.io.*;
import java.util.*;

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

public class DataSplitter {

	class Protein {
		String name = "<untitled>";
		String sequence = "";
		Protein() {}
		Protein(String n, String s) { name = n; sequence = s; }
	}

	public static final double testSetProportion = 0.1;

	public static final int outputLineLen = 70;

	public static final String dataDir = "D:/Courseworks/4C58/cw/data/";

	public static final String [] sourceFiles = new String [] {
											   dataDir + "Cyto_euk.fasta",
											   dataDir + "Extra_euk.fasta",
											   dataDir + "Nuclear.fasta",
											   dataDir + "Mito.fasta"};

	public static final String [] trainDestFiles = new String [] {
											   dataDir + "Cytosol.train.dat",
											   dataDir + "Extracellular.train.dat",
											   dataDir + "Nucleus.train.dat",
											   dataDir + "Mitochondrion.train.dat"};

	public static final String [] validDestFiles = new String [] {
											   dataDir + "Cytosol.valid.dat",
											   dataDir + "Extracellular.valid.dat",
											   dataDir + "Nucleus.valid.dat",
											   dataDir + "Mitochondrion.valid.dat"};

	private ArrayList [] prots = new ArrayList [] {new ArrayList(), new ArrayList(),
												   new ArrayList(), new ArrayList()};

	public DataSplitter() {}

	private void loadProtein(String fname, ArrayList protList) throws IOException {
		int count = 0;
		System.out.print("Loading from " + fname + ". . . ");
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fname)));
		try {
			String s = in.readLine();
			while (null != s) {

				DataSplitter.Protein p = new DataSplitter.Protein();
				p.name = s.trim().substring(1);

				s = in.readLine();
				while (null != s && '>' != s.trim().charAt(0)) {
					p.sequence += s.trim();
					s = in.readLine();
				}
				protList.add(p);
				count++;
			}
		} finally {
			in.close();
		}
		System.out.println(". . . Done. " + count + " proteins loaded.");
	}

	private void loadProteins() throws IOException {
		for (int i = 0; i < 4; i++)
			loadProtein(sourceFiles[i], prots[i]);
	}

	private void randomise() {
		;
	}

	private void saveProtein(DataSplitter.Protein [] protList, String fname) throws IOException {
		System.out.print("Saving " + protList.length + " proteins to " + fname + ". . . ");
		PrintWriter out = new PrintWriter(new FileOutputStream(fname));
		try {
			for (int i = 0; i < protList.length; i++) {
				out.println(">" + protList[i].name);
				int p = 0;
				String s;
				while (p < protList[i].sequence.length()) {
					if (p + outputLineLen < protList[i].sequence.length())
						s = protList[i].sequence.substring(p, p + outputLineLen);
					else
						s = protList[i].sequence.substring(p);
					p += outputLineLen;
					out.println(s);
				}
			}
		} finally {
			out.close();
		}
		System.out.println(". . . Done.");
	}

	private void saveProteins() throws IOException {
		for (int i = 0; i < 4; i++) {

			int validLen = (int) (testSetProportion * (double) prots[i].size());

			DataSplitter.Protein [] train = new DataSplitter.Protein[prots[i].size() - validLen];
			for (int j = 0; j < train.length; j++)
				train[j] = (DataSplitter.Protein) prots[i].get(j);

			DataSplitter.Protein [] valid = new DataSplitter.Protein[validLen];
			for (int j = 0; j < validLen; j++)
				valid[j] = (DataSplitter.Protein) prots[i].get(j + train.length);

			saveProtein(train, trainDestFiles[i]);
			saveProtein(valid, validDestFiles[i]);
		}
	}

	public void exec() {
		try {
			loadProteins();
			randomise();
			saveProteins();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		DataSplitter dataSplitter = new DataSplitter();
		dataSplitter.exec();
	}

}