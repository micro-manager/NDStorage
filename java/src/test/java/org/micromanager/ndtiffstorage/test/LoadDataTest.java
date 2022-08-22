package org.micromanager.ndtiffstorage.test;

import org.micromanager.ndtiffstorage.NDTiffStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.function.BinaryOperator;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

public class LoadDataTest {

    public  static void main (String[] args) {
        String testDataPath = "/Users/henrypinkard/GitRepos/NDTiffStorage/test_data/";

        for (String testDataset : new String[]{"ndtiffv2.0_test", "ndtiffv3.0_test", "ndtiffv2.0_stitched_test", "ndtiffv3.0_stitched_test"}) {
            String path = testDataPath + testDataset;
            NDTiffStorage loadedData = null;
            try {
                loadedData = new NDTiffStorage(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            short[] anImage = (short[]) loadedData.getImage(loadedData.getAxesSet().stream().findFirst().get()).pix;
            assert (anImage[0] > 0);
        }

    }
}
