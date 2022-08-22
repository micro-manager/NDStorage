package org.micromanager.ndtiffstorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Scanner;

public class Test {

    public  static void main (String[] args) {
        InputStream is = ClassLoader.getSystemResourceAsStream("FORMAT_VERSION");
        Scanner myReader = new Scanner(is);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            System.out.println(data);
        }
        myReader.close();
    }
}
