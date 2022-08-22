/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.micromanager.ndtiffstorage;

import java.util.HashMap;
import java.util.Iterator;
import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;

/**
 * This class exists so that important information is not passed in secrectly through summary meteadata, buth
 * rather explicity through constructors. Then the constructors add it into the metadata so that it can be loaded
 * @author henrypinkard
 */
class StorageMD {

   private static final String AXES = "Axes";
   private static final String OVERLAP_X = "GridPixelOverlapX";
   private static final String OVERLAP_Y = "GridPixelOverlapY";
   private static final String TILED_STORAGE = "TiledImageStorage";


   static HashMap<String, Integer> getAxes(JSONObject tags) {
      try {
         JSONObject axes = tags.getJSONObject(AXES);
         Iterator<String> iter = axes.keys();
         HashMap<String, Integer> axesMap = new HashMap<String, Integer>();
         while (iter.hasNext()) {
            String key = iter.next();
            axesMap.put(key, axes.getInt(key));
         }
         return axesMap;
      } catch (JSONException ex) {
         throw new RuntimeException("couldnt create axes");
      }
   }

   public static void setPixelOverlapX(JSONObject smd, int overlap) {
      try {
         smd.put(OVERLAP_X, overlap);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt set pixel overlap tag");

      }
   }

   public static void setPixelOverlapY(JSONObject smd, int overlap) {
      try {
         smd.put(OVERLAP_Y, overlap);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt set pixel overlap tag");

      }
   }

   public static int getPixelOverlapX(JSONObject summaryMD) {
      try {
         return summaryMD.getInt(OVERLAP_X);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt find pixel overlap in image tags");
      }
   }

   public static int getPixelOverlapY(JSONObject summaryMD) {
      try {
         return summaryMD.getInt(OVERLAP_Y);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt find pixel overlap in image tags");
      }
   }
   
      static void setTiledStorage(JSONObject summaryMD, boolean tiled) {
      try {
          summaryMD.put(TILED_STORAGE, tiled);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt find pixel overlap in image tags");
      }
   }

   static boolean getTiledStorage(JSONObject summaryMD) {
      try {
         return summaryMD.getBoolean(TILED_STORAGE);
      } catch (JSONException ex) {
         throw new RuntimeException("TiledStorage");
      }
   }

}
