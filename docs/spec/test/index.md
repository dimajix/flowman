# Tests

Flowman supports test cases as first order entities. These are used for creating self contained unittests to verify
the correctness of the specified data flow. Flowman provides mocking capabilities for mappings and relations such that
physical relations can be replaced by mocked virtual relations and mappings.

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
            
  common_hooks:
    hooks:
      - kind: web
        testSuccess: http://$webhook_host/success&startdate=$URL.encode($start_ts)&enddate=$URL.encode($end_ts)&period=$processing_duration&force=$force

  test_aggregation:
    description: "Test all aggregations"
    extends:
      - common_fixtures
      - common_hooks
        
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

## Fields
* `description` **(optional)** *(type: string)*:
  A textual description of the test

* `environment` **(optional)** *(type: list:string)*:
  A list of `key=value` pairs for defining or overriding environment variables which can be accessed in expressions.

* `targets`: **(optional)** *(type: list:string)*:
  List of targets to be built as part of the test. The targets need to be defined in the regular `targets` section.
  These targets represent the entities to be tested.

* `fixtures`: **(optional)** *(type: map:target)*:
  List of additional targets to be executed as *test fixtures*. These targets are defined directly within the test case
  and are typically used to produce phyiscal data, which then is pciked up by some test cases.
  
* `overrideMappings`: **(optional)** *(type: map:mapping)*:
  This section allows you to override existing mappings with new definitions. Typically this is used for mocking the
  output of some mappings by replacing those with [`values`](../mapping/values.md) or [`mock`](../mapping/mock.md) 
  mappings. You can also specify new mappings in this section.
  
* `overrideRelations`: **(optional)** *(type: map:relation)*:
  This section allows you to override existing relations with new definitions. You can also specify new relations in 
  this section.
  
* `assertions`: **(optional)** *(type: map:assertion)*:
  This section contains the set of [assertions](../assertion/index.md) to be executed. The test is considered to have
  failed if a single assertion has failed.

* `hooks` **(optional)** *(type: list:hook)*:
  A list of [hooks](../hooks/index.md) which will be called before and after each test and assertion is executed. Hooks 
  provide some ways to notify external systems (or possibly plugins) about the current execution status of tests.


## Sub Pages
```eval_rst
.. toctree::
   :maxdepth: 1
   :glob:

   *
```
