package com.journaldev.sparkdemo.fileOps;



import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.HashMap;
import java.util.Scanner; // Import the Scanner class to read text files
import java.util.regex.Pattern;


public class FileOperations {

        public HashMap<String,String> readTagFile(String fileName) {
            HashMap<String,String> tagFileContents = new HashMap<>();
            try {


                File myObj = new File(fileName);
                Scanner myReader = new Scanner(myObj);
                //while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                //System.out.println(data);
                String[] value_split = data.split("\\|");
                //System.out.println("asfdsa" + value_split[0]);
                //System.out.println(value_split[1]);

                tagFileContents.put("fileName",value_split[0]);
                tagFileContents.put("recordCount",value_split[1]);




                //}
                myReader.close();
            } catch (FileNotFoundException e){
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
            return tagFileContents;
        }

        public static void main(String[] args) {
            FileOperations fileOps = new FileOperations();
            HashMap<String,String> tagFileContents = null;


            tagFileContents = fileOps.readTagFile("scenarios\\aus-capitals.tag");

            tagFileContents.entrySet().forEach(entry -> {
                System.out.println(entry.getKey() + " " + entry.getValue());
            });
        }
    }


