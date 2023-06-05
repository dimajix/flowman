# Report Hook

The `report` will create a textual report file containing information on the execution. As with all hooks, it can be
either added on the namespace level or on the job level.

## Example
```yaml
job:
  main:
    hooks:
      - kind: report
        location: ${project.basedir}/generated-report.txt
        metrics:
          # Define common labels for all metrics
          labels:
            project: ${project.name}
          metrics:
            # This metric contains the number of records per output
            - name: output_records
              selector:
                name: target_records
                labels:
                  category: target
              labels:
                target: ${name}
            # This metric contains the processing time per output
            - name: output_time
              selector:
                name: target_runtime
                labels:
                  category: target
              labels:
                target: ${name}
            # This metric contains the overall processing time
            - name: processing_time
              selector:
                name: job_runtime
                labels:
                  category: job
```
