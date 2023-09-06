package org.micromanager.ndtiffstorage;

/**
 * Used to signal that an image has been written to disk
 */
@Deprecated
public interface ImageWrittenListener {

   public void imageWritten(IndexEntryData ied);

   /**
    * Block until this listenter is shut down and all its resources freed
    */
   @Deprecated
   public void awaitCompletion();

}
