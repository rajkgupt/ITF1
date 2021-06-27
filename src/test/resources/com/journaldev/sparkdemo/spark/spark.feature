

Feature: File Integrity checks
  These checks are typically performed to ensure that the file
  received from a source system is wholly complete.

  Scenario: Valid file – all checks are successful
    Given I have a DATA file named "scenarios/aus-capitals.csv"
    And I have a TAG file named "scenarios/aus-capitals.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-1.csv"
    Then the program should exit with RETURN CODE of "0"


  Scenario: Invalid file – record count does not match
    Given I have a DATA file named "scenarios/aus-capitals.csv"
    And I have a TAG file named "scenarios/aus-capitals-invalid-1.tag"
    And I have a SCHEMA file named "scenarios/aus-capitals.json"
    When I execute the application with output "output/sbe-1-2.csv"
    Then the program should exit with RETURN CODE of "1"