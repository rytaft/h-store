package edu.brown.hstore.reconfiguration;

public abstract class ReconfigurationConstants {

  public static enum ReconfigurationProtocols {
    STOPCOPY,
    LIVEPULL
  }
  
    public static long MAX_TRANSFER_BYTES = 1024L*1024*1024*9; //9 GB - VOTER HACK
    public static long MIN_TRANSFER_BYTES = 1024L*1024*9; //9 MB
}
