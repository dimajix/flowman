# Script Expressions

Flowman allows to use *expressions* at many places in the YAML specification files. For example you can reference 
variables defined in the `environment` section or also access a set of predefined objects inside values.

Flowman uses [Apache Velocity](https://velocity.apache.org/) as the template engine. Please also read 
[its documentation](https://velocity.apache.org/engine/2.2/) for advanced feature like 
[conditional expression](https://velocity.apache.org/engine/2.2/vtl-reference.html#ifelseifelse-output-conditional-on-truth-of-statements) 
or [recursive evaluation](https://velocity.apache.org/engine/2.2/vtl-reference.html#evaluate-dynamically-evaluates-a-string-or-reference).

## Simple Expressions
You can access the contents of a variable via a dollar sign (`$`), like for example:
```
mappings:
  weather:
    kind: read
    source: $weather_source
```
This example uses a variable `weather_source` to define which relation to read form. This way
different environments can be supported by simply setting different values for that variable.

Variables can be set globally in a project's `environment` section or in a profile of a
project (which then has to be activated) or on the command line.


## Predefined Objects
Flowman also provides some predefined objects, which are available in the expression language.
These objects either simply hold some data (like the location of the project), or they also 
provide some functions (for working with date and time).

### `project`
* `project.basedir`
* `project.filename`
* `project.name`
* `project.version`

### `Integer`
* `Integer.parse(string)` or `Integer.valueOf(string)`

### `Float`
* `Float.parse(string)` or `Float.valueOf(string)`

### `LocalDate`
* `LocalDate.parse(string)` or `LocalDateTime.valueOf(string)`
* `LocalDate.ofEpochSeconds(int)`
* `LocalDate.format(string, string)`
* `LocalDate.addDays(string, int)`
* `LocalDate.addWeeks(string, int)`
* `LocalDate.addMonths(string, int)`
* `LocalDate.addYears(string, int)`

### `LocalDateTime`
* `LocalDateTime.parse(string)` or `LocalDateTime.valueOf(string)`
* `LocalDateTime.ofEpochSeconds(int)`
* `LocalDateTime.format(string, string)`
* `LocalDateTime.add(string, string)`
* `LocalDateTime.subtract(string, string)`
* `LocalDateTime.addSeconds(string, int)`
* `LocalDateTime.addMinutes(string, int)`
* `LocalDateTime.addHours(string, int)`
* `LocalDateTime.addDays(string, int)`
* `LocalDateTime.addWeeks(string, int)`
* `LocalDateTime.addMonths(string, int)`
* `LocalDateTime.addYears(string, int)`


### `Timestamp`
* `Timestamp.parse(string)` or `Timestamp.valueOf(string)`
* `Timestamp.ofEpochSeconds(int)`

### `Duration`
* `Duration.ofDays(int)`
* `Duration.ofHours(int)`
* `Duration.ofMinutes(int)`
* `Duration.ofSeconds(int)`
* `Duration.ofMillis(int)`
* `Duration.between(string,string)`
* `Duration.parse(string)` or `Duration.valueOf(string)`

### `Period`
* `Period.ofYears(int)`
* `Period.ofMonths(int)`
* `Period.ofWeeks(int)`
* `Period.ofDays(int)`
* `Period.parse(string)` or `Period.valueOf(string)`

### `System`
* `System.getenv(string)` or `System.getenv(string,string)`
* `System.getProperty(string)` or `System.getProperty(string,string)`

### `String`
* `String.concat(string,string)`
