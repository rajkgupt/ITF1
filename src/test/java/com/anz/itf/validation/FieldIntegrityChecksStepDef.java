package com.anz.itf.validation;

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
import org.apache.spark.sql.functions;

import java.util.HashMap;

public class FieldIntegrityChecksStepDef {
    HashMap<String, String > inputArgsMap = new HashMap<>();



    @Given("I have a DATA named {string}")
    public void i_have_a_data_named(String string) {
        inputArgsMap.put("data",string);

    }

    @Given("I have a TAG file named1 {string}")
    public void i_have_a_tag_file_named1(String string) {
        inputArgsMap.put("tag",string);
    }

    @Given("I have a SCHEMA file named1 {string}")
    public void i_have_a_schema_file_named1(String string) {
        inputArgsMap.put("schema",string);
    }

    @When("I execute the application with output1 {string}")
    public void i_execute_the_application_with_output1(String string) {
        inputArgsMap.put("output", string);

        SparkSession spark = SparkSession
                .builder()
                .config("spark.master","local")
                .appName("JavaWordCount")
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

        Dataset<Row>newDf = inputFileDF.withColumn("dirty_flag",functions.when(functions.col("_corrupt_record").isNotNull(), 1)
                .otherwise(0));
        newDf = newDf.drop("_corrupt_record");
        newDf.show();

    }

    @Then("the program should exit with RETURN CODE1 of {string}")
    public void the_program_should_exit_with_return_code1_of(String string) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
    }
    @Then("{string} should match {string}")
    public void should_match(String string, String string2) {
        // Write code here that turns the phrase above into concrete actions
        //throw new io.cucumber.java.PendingException();
    }

}
