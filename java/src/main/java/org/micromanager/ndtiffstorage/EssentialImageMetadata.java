package org.micromanager.ndtiffstorage;

/**
 * Width, height, pixel type etc.
 */
public class EssentialImageMetadata {

    public final int width, height, bitDepth;
    public final boolean rgb;

    public EssentialImageMetadata(int w, int h, int bd, boolean isRGB) {
        width = w;
        height = h;
        bitDepth = bd;
        rgb = isRGB;
    }
}
