# Testing with Flowman

Testing data pipelines often turns out to be a difficult undertaking, since the pipeline relies on external data
sources which need to be mocked. Fortunately, Flowman natively supports writing `tests`, which in turn support simple
mocking of relations and/or mappings. This allows you to easily test the whole processing logic or even to test
only some subsets of functionality represented by some mapping by mocking the output of its input mappings and just 
checking the results of the mapping under test.

## Flowman Tests

Let's have a look at the following example:
```yaml
tests:
  test_facts:
    environment:
      - year=2013

    overrideMappings:
      measurements:
        kind: mock
        records:
          - year: $year
            date: $year-01-02
            time: 0100
            usaf: 999999
            wban: 63897
            wind_direction_qual: 9
            wind_speed_qual: 9
            air_temperature_qual: 9
          - year: $year
            date: $year-01-02
            time: 0100
            usaf: 99999
            wban: 63897
            wind_direction_qual: 9
            wind_speed_qual: 9
            air_temperature_qual: 9

      stations:
        kind: mock
        records:
          - usaf: 999999
            wban: 63897
            country: US
          - usaf: 999999
            wban: 1
            country: DE

    targets:
      - validate_stations_raw

    assertions:
      measurements_joined:
        kind: sql
        description: "Measurements are joined correctly"
        tests:
          - query: "SELECT year,usaf,wban,country FROM measurements_joined"
            expected:
              - [$year,999999,63897,US]
              - [$year,99999,63897,null]
```

In the example above, the mapping `measurements_joined` (not shown here) is tested inside the `SELECT` statement of the
`assertions` block at the end. To be able to run this statement without accessing external data sources, some
input relations (namely `measurements` and `stations`) have been mocked in the `overrideMappings` section.

Please find more details in the [testing documentation](../spec/test/index.md)


## Running Tests

The easiest way to execute tests is to use the [Flowman Shell](../cli/flowshell.md), which provides a simple command
`test run`, which will run all tests defined in your project.


## Java/Scala Unittests

Flowman now also includes a `flowman-testing` library which allows one to write lightweight unittests using either Scala
or Java. The library provides some simple test runner for executing tests and jobs specified as usual in YAML files. 
