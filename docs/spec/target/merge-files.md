# Merge Files Target

The `mergeFiles` target merges all files within a source directory to a single target file. This only makes sense if
the file format allows appending by simple bytewise concatenation. This is the case for textual files, like CSV and
TSV files.

## Example:
```yaml
targets:
  csv_merge:
    kind: mergeFiles
    source: "s3://my-bucket/my-spark-output/"
    target: "file:///srv/exports/my-export.csv"
    overwrite: true
```

## Fields
 * `kind` **(mandatory)** *(string)*: `mergeFiles`
 * `description` **(optional)** *(type: string)*:
   Optional descriptive text of the build target
 * `source` **(mandatory)** *(string)*: Source directory containing all files to be concatenated
 * `target` **(optional)** *(string)*: Name of single target file
 * `overwrite` **(optional)** *(boolean)* *(default: true)*: 


## Supported Execution Phases
* `BUILD`
* `VERIFY`
* `TRUNCATE`
* `DESTROY`

Read more about [execution phases](../../lifecycle.md).
