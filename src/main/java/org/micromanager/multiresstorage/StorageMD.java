/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.micromanager.multiresstorage;

import java.util.HashMap;
import java.util.Iterator;
import mmcorej.org.json.JSONException;
import mmcorej.org.json.JSONObject;

/**
 *
 * @author henrypinkard
 */
class StorageMD {

   private static final String CHANNEL_NAME = "Channel";
   private static final String AXES = "Axes";
   private static final String GRID_COL = "GridColumnIndex";
   private static final String GRID_ROW = "GridRowIndex";
   private static final String OVERLAP_X = "GridPixelOverlapX";
   private static final String OVERLAP_Y = "GridPixelOverlapY";
   private static final String FULL_RES_TILE_HEIGHT = "FullResTileHeight";
   private static final String FULL_RES_TILE_WIDTH = "FullResTileWidth";
   private static final String TILED_STORAGE = "TiledImageStorage";


   static void setGridRow(JSONObject smd, long gridRow) {
      try {
         smd.put(GRID_ROW, gridRow);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt set grid row");

      }
   }

   static void setGridCol(JSONObject smd, long gridCol) {
      try {
         smd.put(GRID_COL, gridCol);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt set grid row");

      }
   }

   static long getGridRow(JSONObject smd) {
      try {
         return smd.getLong(GRID_ROW);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt set grid row");

      }
   }

   static long getGridCol(JSONObject smd) {
      try {
         return smd.getLong(GRID_COL);
      } catch (JSONException ex) {
         throw new RuntimeException("Couldnt set grid row");

      }
   }

   public static String getChannelName(JSONObject map) {
      try {
         return map.getString(CHANNEL_NAME);
      } catch (JSONException ex) {
         throw new RuntimeException("Missing channel index tag");
      }
   }

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

   public static void setFullResTileWidth(JSONObject summaryMD_, int fullResTileWidthIncludingOverlap_) {
      try {
         summaryMD_.put(FULL_RES_TILE_WIDTH, fullResTileWidthIncludingOverlap_);
      } catch (JSONException ex) {
         throw new RuntimeException(ex);
      }
   }

   public static void setFullResTileHeight(JSONObject summaryMD_, int fullResTileHeightIncludingOverlap_) {
      try {
         summaryMD_.put(FULL_RES_TILE_HEIGHT, fullResTileHeightIncludingOverlap_);
      } catch (JSONException ex) {
         throw new RuntimeException(ex);
      }
   }

   public static int getFullResTileWidth(JSONObject summaryMD) {
      try {
         return summaryMD.getInt(FULL_RES_TILE_WIDTH);
      } catch (JSONException ex) {
         throw new RuntimeException(ex);
      }
   }

   public static int getFullResTileHeight(JSONObject summaryMD) {
      try {
         return summaryMD.getInt(FULL_RES_TILE_HEIGHT);
      } catch (JSONException ex) {
         throw new RuntimeException(ex);
      }
   }
}
