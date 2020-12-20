/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.micromanager.multiresstorage;

import java.awt.Point;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import mmcorej.TaggedImage;
import mmcorej.org.json.JSONObject;

/**
 * Minimal Data storage API. The interface does not include any of the methods for multi-resolution
 * data, which are in the MultiresStorageAPI interface
 *
 * @author henrypinkard
 */
public interface StorageAPI {

   /**
    * Add an image into storage
    *
    * @param taggedImg
    * @param axes
    */
   public void putImage(TaggedImage taggedImg, HashMap<String, Integer> axes);

   /**
    * Is this dataset finished writing and now read only?
    *
    * @return 
    */
   public boolean isFinished();

   /**
    * Set display settings for storage. No particular structure required as the
    * storage class will only save and load them but not do anything with them.
    *
    * @param displaySettings 
    */
   public void setDisplaySettings(JSONObject displaySettings);

   /**
    * The display settings for this dataset
    * Note: the storage only specifies that these are a JSON object, 
    * but nothing about their format. It only stores them
    * @return 
    */
   public JSONObject getDisplaySettings();

   /**
    * 
    * @return the summary metadata for this dataset. When creating new storage instances, this will
    * have been supplied in the constructor.
    */
   public JSONObject getSummaryMetadata();

   /**
    * Called when no more data will be written to this dataset (but reading still allowed)
    */
   public void finishedWriting();

   /**
    * Get the path to the top level folder where this dataset is or null if its not saved to disk
    * @return 
    */
   public String getDiskLocation();

   /**
    * Release all resources
    */
   public void close();

   /**
    * [x_min, y_min, x_max, y_max] pixel bounds where data has been acquired (can be negative).
    * In the simplest case this will be [0, 0, width, height]
    * @return 
    */
   public int[] getImageBounds();

   /**
    * Get a single image from full resolution data
    * 
    * @param axes 
    * @return 
    */
   public TaggedImage getImage(HashMap<String, Integer> axes);

   /**
    * Get a set containing all image axes in this dataset
    *
    * @return 
    */
   public Set<HashMap<String, Integer>> getAxesSet();

   /**
    * Return a unqiue name associated with this data storage instance. For instances
    * on disk, this will be the name supplied to the storage in a constructer, plus
    * some suffix (i.e. "_1", "_2", etc.) to differentiate the corrsponding files
    * on disk if multiple instances are created with the same name argument.
    * @return
    */
   public String getUniqueAcqName();


}
