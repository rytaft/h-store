package org.voltdb.exceptions;

import java.util.List;

import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;

/**
 * Exceptions thrown during the Reconfiguration process
 * @author vaibhav
 *
 */
public class ReconfigurationException extends SerializableException {

  public static final long serialVersionUID = 0L;
  public ReconfigurationProtocols reconfigurationProtocols;
  
  public enum ExceptionTypes {
      TUPLES_MIGRATED_OUT,
      TUPLES_NOT_MIGRATED,
      BOTH
  };
  
  public ExceptionTypes exceptionType;
  public List<ReconfigurationRange<? extends Comparable<?>>> dataMigratedOut = null;
  public List<ReconfigurationRange<? extends Comparable<?>>> dataNotYetMigrated = null;
  
  public ReconfigurationException(ReconfigurationProtocols 
      reconfigurationProtocols){
    this.reconfigurationProtocols = reconfigurationProtocols;
  }
  
  public ReconfigurationProtocols getReconfigurationProtocols(){
    return this.reconfigurationProtocols;
  }
  
}
