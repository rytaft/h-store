/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.twitter.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class TwitterGraphLoader {

	String filename;
	DataInputStream dis = null;
	Random r = null;
	static final double READ_WRITE_RATIO = 11.8; // from
													// http://www.globule.org/publi/WWADH_comnet2009.html

	public TwitterGraphLoader(String filename) throws FileNotFoundException {
		r = new Random();
		this.filename = filename;

		if(filename==null || filename.isEmpty())
			throw new FileNotFoundException("You must specify a filename to instantiate the TwitterGraphLoader... (probably missing in your workload configuration?)");

		File file = new File(filename);
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		fis = new FileInputStream(file);

		// Here BufferedInputStream is added for fast reading.
		bis = new BufferedInputStream(fis);
		dis = new DataInputStream(bis);
		dis.mark(1024 * 1024 * 1024);
	
	}

	public synchronized TwitterGraphEdge nextEdge() throws IOException {
		if (dis.available() == 0)
			dis.reset();

		return readNextEdge();
	}

	public TwitterGraphEdge readNextEdge() throws IOException {
		String line = dis.readLine();
		String[] sa = line.split("\\s+");
		int followee = Integer.parseInt(sa[0]);
		int follower = Integer.parseInt(sa[1]);
		
		return new TwitterGraphEdge(follower,followee);
	}

	public ArrayList<TwitterGraphEdge> readAll() throws IOException {
		ArrayList<TwitterGraphEdge> edges = new ArrayList<TwitterGraphEdge>();

		while (dis.available() > 0) {
			edges.add(readNextEdge());
		}

		return edges;
	}
	
	public boolean hasNext() throws IOException {
		return dis.available() > 0;
	}

	public void close() throws IOException {
		dis.close();
	}

}
