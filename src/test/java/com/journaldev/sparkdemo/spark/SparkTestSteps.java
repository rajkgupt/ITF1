package com.journaldev.sparkdemo.spark;
import com.journaldev.sparkdemo.fileOps.FileOperations;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.math.BigInt;
import static org.junit.Assert.*;

import java.util.HashMap;

public class SparkTestSteps {
    HashMap<String, String > inputArgsMap = new HashMap<>();
    String dataFileNameFromTagFile = null;
    Long totalRecordCountFromDF = null;
    Long recordCountFromTagFile = null;
    String fileNameWithFolderFromInputCommand = null;
    String fileNameWithoutFolderFromInputCommand = null;
    int exitCode;

    public void initializeInputParameters() {



    }


    @Given("I have a DATA file named {string}")
    public void i_have_a_data_file_named(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();

        inputArgsMap.put("data",string);

    }
    @Given("I have a TAG file named {string}")
    public void i_have_a_tag_file_named(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
        inputArgsMap.put("tag",string);
    }
    @Given("I have a SCHEMA file named {string}")
    public void i_have_a_schema_file_named(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
        inputArgsMap.put("schema",string);
    }
    @When("I execute the application with output {string}")
    public void i_execute_the_application_with_output(String string) {
        inputArgsMap.put("output",string);


        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("JavaWordCount")
                .getOrCreate();

        StructType schema = new StructType(new StructField[]{
                new StructField("State/Territory"     , DataTypes.StringType ,false, Metadata.empty()),
                new StructField("Capital"   ,DataTypes.StringType  ,false, Metadata.empty()),
                new StructField("City Population"     ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("State/Territory Population"   ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("Percentage"   ,DataTypes.IntegerType  ,false, Metadata.empty()),
                new StructField("Established"   ,DataTypes.DateType  ,false, Metadata.empty()),
                new StructField("_corrupt_record"   ,DataTypes.StringType  ,false, Metadata.empty())
        });

        Dataset<Row> inputFileDF  = spark.read().format("csv")
                .option("header", "true")
                .schema(schema)
                .load("C:\\raj\\Spark\\solution\\scenarios\\aus-capitals.csv");

        //df.registerTempTable("tempTable");
        totalRecordCountFromDF = (Long) inputFileDF.count();



        //Get FileName and recordCount from tag file
        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));

        dataFileNameFromTagFile = tagFileContents.get("fileName");
        recordCountFromTagFile = Long.parseLong(tagFileContents.get("recordCount"));

        //assertEquals(totalRecordCountFromDF,recordCountFromTagFile);
        //assertEquals(inputArgsMap.get("data"),dataFileNameFromTagFile);

        fileNameWithFolderFromInputCommand = inputArgsMap.get("data");
        String[] value_split = fileNameWithFolderFromInputCommand.split("/");
        fileNameWithoutFolderFromInputCommand = value_split[1];

        System.out.println("Exiting When.......");
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
        spark.stop();

    }

    @Then("the program should exit with RETURN CODE of {string}")
    public void the_program_should_exit_with_return_code_of(String string) {
        //System.exit(0);
        //assertEquals(fileNameWithoutFolderFromInputCommand,dataFileNameFromTagFile);
        //assertEquals(totalRecordCountFromDF,recordCountFromTagFile);

        if (fileNameWithoutFolderFromInputCommand.equals(dataFileNameFromTagFile)) {
            if (totalRecordCountFromDF == recordCountFromTagFile) {
                //System.exit(0);
                exitCode = 0;
                assertTrue("0",true);

            } else {
                //System.exit(1);
                assertTrue("1",true);
            }
        } else {
            assertTrue("2",true);
        }

        //System.exit(10);


    }
    @After("@tag1")
    public void testEnd(){
        if (exitCode == 0) {
            System.out.println("Exiting with status 0");
            System.exit(0);
        } else {
            System.out.println("Exiting with status 1");
            System.exit(11);
        }
    }
}
