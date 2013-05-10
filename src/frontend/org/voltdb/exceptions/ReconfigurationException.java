package org.voltdb.exceptions;

import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;

/**
 * Exceptions thrown during the Reconfiguration process
 * @author vaibhav
 *
 */
public class ReconfigurationException extends SerializableException {

  public static final long serialVersionUID = 0L;
  public ReconfigurationProtocols reconfigurationProtocols;
  
  public ReconfigurationException(ReconfigurationProtocols 
      reconfigurationProtocols){
    this.reconfigurationProtocols = reconfigurationProtocols;
  }
  
  public ReconfigurationProtocols getReconfigurationProtocols(){
    return this.reconfigurationProtocols;
  }
  
}
