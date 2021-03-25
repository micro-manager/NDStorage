package org.micromanager.multiresstorage;

/**
 * Used to signal that an image has been written to disk
 */
public interface ImageWrittenListener {

   public void imageWritten(IndexEntryData ied);

}
