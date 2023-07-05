Feature: TestAT basic example

    Scenario Outline: Test example process exits successfully
      Given a config file <config_file>
      When execute example app file in PySpark
      Then result should be <exit_code>

      Scenarios:
        | config_file      | exit_code |
        | application.conf | 0         |
