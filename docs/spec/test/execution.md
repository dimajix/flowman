# Flowman Test Execution

The different aspects of each test are execution in a specific order by Flowman. 

1. A test environment is setup by adding or modifying any variable specified in the `environment` section
2. All relations and mappings specified as overrides are created to replace and extend the original entities.
3. All `targets` and `fixtures` are executed. Data dependencies are used to determine a correct execution order.
  The execution includes the `CREATE`, `BUILD` and `VERIFY` phases.
4. All `assertions` are executed   
5. All `targets` and `fixtures` are cleaned up by executing the `DESTROY` phase.



## Example
```yaml
tests:
  common_fixtures:
    overrideMappings:
      mapping_a:
        kind: mock
        records:
          - [1,"cat","black"]
          - [2,"dog","brown"]
            
    overrideRelations:
      raw_data:
        kind: mock
        records:
          - "042599999963897201301010000I"

  test_aggregation:
    description: "Test all aggregations"
    extends:
      - common_fixtures
        
    environment:
      - some_value=12  

    targets:
      - build_cube
      
    fixtures:
      prepare_additional_data:
        kind: relation
        relation: additional_data
        mapping: mapping_a

    assertions:
      measurements_extracted:
        kind: sql
        description: "Measurements are extracted correctly"
        tests:
          - query: "SELECT * FROM measurements_extracted"
            expected:
              - [1,63897,999999,10.6,2013-01-01,0.9,1,124,CRN05,1,0000,H]
              - [1,63897,999999,10.6,2013-01-01,1.5,1,124,CRN05,1,0005,H]
          - query: "SELECT COUNT(*) FROM measurements_extracted"
            expected: 2
```

In the example above, the following steps are executed:

1. A new execution environment is created containing all original variables plus the `some_value` variable.
2. The override mapping `mapping_a` and override relation `raw_data` are added to the execution environment.
3. The targets and fixtures `build_cube` and `prepare_additional_data` are executed with phases `CREATE`, `BUILD` and
   `VERIFY`.
4. The assertion `measurement_extracted` is run with all tests executed.
5. The targets and fixtures `build_cube` and `prepare_additional_data` are cleaned up with phase `DESTROY`.
