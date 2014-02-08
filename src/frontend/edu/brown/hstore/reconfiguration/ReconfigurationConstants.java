package edu.brown.hstore.reconfiguration;

public abstract class ReconfigurationConstants {

  public static enum ReconfigurationProtocols {
    STOPCOPY,
    LIVEPULL
  }
  
  public static long MAX_TRANSFER_BYTES = 1024*1024*1024*10*9; //90 GB
  public static long MIN_TRANSFER_BYTES = 1024*1024*1024*10*9; //90 GB
}
