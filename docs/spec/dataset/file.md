# Flowman File Dataset

The *file dataset* can be used for reading data from a shared file system

## Example
```yaml
kind: file
format: json
location: "${project.basedir}/test/data/results/${relation}/data.json"
schema:
  kind: spark
  file: "${project.basedir}/test/data/results/${relation}/schema.json"
```
