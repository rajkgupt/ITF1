package com.anz.itf.validation;
import com.anz.itf.utils.FileOperations;
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

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;

public class FileIntegrityChecksStepDef {
    HashMap<String, String > inputArgsMap = new HashMap<>();
    String dataFileNameFromTagFile = null;
    Long totalRecordCountFromDF = null;
    Long recordCountFromTagFile = null;
    String fileNameWithFolderFromInputCommand = null;
    String fileNameWithoutFolderFromInputCommand = null;
    long missingOrAdditionalColumnCount = 0;

    int exitCode;
    long primaryKeyCount = 0;

    @Given("I have a DATA file named {string}")
    public void i_have_a_data_file_named(String string) {
        inputArgsMap.put("data",string);

    }
    @Given("I have a TAG file named {string}")
    public void i_have_a_tag_file_named(String string) {
        inputArgsMap.put("tag",string);
    }
    @Given("I have a SCHEMA file named {string}")
    public void i_have_a_schema_file_named(String string) {
        inputArgsMap.put("schema",string);
    }
    @When("I execute the application with output {string}")
    public void i_execute_the_application_with_output(String string) {
        inputArgsMap.put("output",string);

        inputArgsMap.entrySet().forEach(entry -> {
            System.out.println(entry.getKey() + " " + entry.getValue());
        });


        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("FileIntegrityChecks")
                .getOrCreate();

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

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
                .load(inputArgsMap.get("data")).cache();


        //Prep to perform FileName & Record count checks
        // Get FileName and recordCount from tag file
        FileOperations fileOperations = new FileOperations();
        HashMap<String,String> tagFileContents = fileOperations.readTagFile(inputArgsMap.get("tag"));


        //Operation for recordCount check
        totalRecordCountFromDF = inputFileDF.count();
        recordCountFromTagFile = Long.parseLong(tagFileContents.get("recordCount"));
        System.out.println("totalRecordCountFromDF calculated is:" + totalRecordCountFromDF);
        System.out.println("recordCountFromTagFile found is:" + recordCountFromTagFile);


        //Operation for fileName check
        fileNameWithFolderFromInputCommand = inputArgsMap.get("data");
        String[] fileFolderNamesArray = fileNameWithFolderFromInputCommand.split("/");
        fileNameWithoutFolderFromInputCommand = fileFolderNamesArray[1];
        dataFileNameFromTagFile = tagFileContents.get("fileName");
        System.out.println("fileNameWithoutFolderFromInputCommand calculated is:" + fileNameWithoutFolderFromInputCommand);
        System.out.println("dataFileNameFromTagFile found is:" + dataFileNameFromTagFile);


        //Prep to perform Primary key & Missing or additional columns check
        //Register the dataframe with table
        inputFileDF.createOrReplaceTempView("ausInputFileTable");

        //Operation for primary key violation check
        Dataset<Row> primaryKeyCountDF = spark.sql("WITH " +
                "t AS (select `State/Territory`,count(*) as count1 from ausInputFileTable group by `State/Territory` having count(*) > 1)," +
                " t2 AS ( SELECT count(*) as count2 FROM t where count1 <> 1 )" +
                "SELECT count2 FROM t2");
        Row primaryKeyCountRow = primaryKeyCountDF.select(col( "count2")).first();
        primaryKeyCount = primaryKeyCountRow.getAs("count2");
        System.out.println("primaryKeyCount calculated is:" + primaryKeyCount);



        //Operations to check for missing or additional columns
        Dataset<Row> missingOrAdditionalColumnCountDF =
                spark.sql("select count(*) as count1 from ausInputFileTable where _corrupt_record is not null");
        Row missingOrAdditionalColumnCountRow = missingOrAdditionalColumnCountDF.select(col( "count1")).first();
        missingOrAdditionalColumnCount = missingOrAdditionalColumnCountRow.getAs("count1");
        System.out.println("missingOrAdditionalColumnCount calculated is:" + missingOrAdditionalColumnCount);

        inputFileDF.show();

        spark.stop();

    }

    @Then("the program should exit with RETURN CODE of {string}")
    public void the_program_should_exit_with_return_code_of(String string) {


        assertEquals(fileNameWithoutFolderFromInputCommand, dataFileNameFromTagFile); //fileName check
        assertEquals(totalRecordCountFromDF, recordCountFromTagFile); //record count check
        assertEquals(primaryKeyCount,0); //primary key count check
        assertEquals(missingOrAdditionalColumnCount, 0); //missing or additional column records check


        /*
        if (fileNameWithoutFolderFromInputCommand.equals(dataFileNameFromTagFile)) {
            if (totalRecordCountFromDF == recordCountFromTagFile) {
                if (primaryKeyCount == 0) {
                    if (missingOrAdditionalColumnCount == 0) {
                        assertTrue("0",true);
                    }
                }
            }
        }*/

    }
    @After("@tag1")
    public void testEnd(){
        if (exitCode == 0) {
            System.out.println("Exiting with status 0");
            //System.exit(0);
        } else {
            System.out.println("Exiting with status 1");
            //System.exit(11);
        }
    }
}
