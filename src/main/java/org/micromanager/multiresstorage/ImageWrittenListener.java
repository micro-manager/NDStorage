package org.micromanager.multiresstorage;

/**
 * Used to signal that an image has been written to disk
 */
public interface ImageWrittenListener {

   public void imageWritten(IndexEntryData ied);

   /**
    * Block until this listenter is shut down and all its resources freed
    */
   public void awaitCompletion();

}
