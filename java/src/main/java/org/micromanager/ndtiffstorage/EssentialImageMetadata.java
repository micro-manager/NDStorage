package org.micromanager.ndtiffstorage;

/**
 * Width, height, pixel type etc.
 */
public class EssentialImageMetadata {

    public final int width, height, byteDepth;
    public final boolean rgb;

    public EssentialImageMetadata(int w, int h, int byted, boolean isRGB) {
        width = w;
        height = h;
        byteDepth = byted;
        rgb = isRGB;
    }
}
