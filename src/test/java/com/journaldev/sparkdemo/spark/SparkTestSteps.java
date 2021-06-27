package com.journaldev.sparkdemo.spark;
import com.journaldev.sparkdemo.fileOps.FileOperations;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.math.BigInt;
import static org.junit.Assert.*;

import java.util.HashMap;

public class SparkTestSteps {
    HashMap<String, String > inputArgsMap = new HashMap<>();

    public void initializeInputParameters() {
        inputArgsMap.put("schema","scenarios\\aus-capitals.json");
        inputArgsMap.put("data","scenarios\\aus-capitals.csv");
        inputArgsMap.put("tag","scenarios\\aus-capitals.tag");
    }


    @Given("I have a DATA file named {string}")
    public void i_have_a_data_file_named(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
        inputArgsMap.put("output","scenarios\\sbe-1-1.csv");
    }
    @Given("I have a TAG file named {string}")
    public void i_have_a_tag_file_named(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
    }
    @Given("I have a SCHEMA file named {string}")
    public void i_have_a_schema_file_named(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
    }
    @When("I execute the application with output {string}")
    public void i_execute_the_application_with_output(String string) {


        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("JavaWordCount")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("header","true")
                .option("schema",inputArgsMap.get("schema"))
                .load(inputArgsMap.get("data"));

        df.createOrReplaceGlobalTempView("tempTable");
        Long totalRecordCountFromDF = (Long) spark.sql("select count(*) from tempTable").count();

        //Get FileName and recordCount from tag file
        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));

        String dataFileNameFromTagFile = tagFileContents.get("fileName");
        Long recordCountFromTagFile = Long.parseLong(tagFileContents.get("recordCount"));

        assertEquals(totalRecordCountFromDF,recordCountFromTagFile);

        assertEquals(inputArgsMap.get("tag"),dataFileNameFromTagFile);
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();


    }
}
