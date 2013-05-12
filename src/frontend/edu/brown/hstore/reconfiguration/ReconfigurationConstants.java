package edu.brown.hstore.reconfiguration;

public abstract class ReconfigurationConstants {

  public static enum ReconfigurationProtocols {
    STOPCOPY,
    LIVEPULL
  }
  
  public static final long MAX_TRANSFER_BYTES = 1024*1024*10; //10 MB
}
