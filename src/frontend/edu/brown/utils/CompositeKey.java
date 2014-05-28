/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.util.List;

/**
 * Pack multiple values into a single long using bit-shifting
 * @author pavlo
 */
public class CompositeKey<T extends Comparable<T>> implements Comparable<CompositeKey<T>> {
    
    private List<T> values;
    
    public CompositeKey(List<T> values) {
    	this.values = values;
    }
    
    @Override
    public int compareTo(CompositeKey<T> o) {
        for (int i = 0; i < o.values.size() && i < this.values.size(); i++) {
        	if (this.values.get(i).compareTo(o.values.get(i)) < 0) {
        		return -1;
        	} else if (this.values.get(i).compareTo(o.values.get(i)) > 0) {
        		return 1;
        	}
        }
        
        if (this.values.size() < o.values.size()) {
        	return -1;
        } else if (this.values.size() > o.values.size()) {
        	return 1;
        }
        return 0;
    }
    
    public List<T> getValues() {
    	return this.values;
    }
    
    public String toString() {
    	String ret = "";
    	boolean first = true;
    	for (T val : values) {
    		if (!first) {
    			ret += ", ";
    		} else {
        		first = false;    			
    		}
    		
    		ret += val;
    	}
    	
    	return ret;
    }

}
