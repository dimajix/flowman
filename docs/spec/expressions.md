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
* `LocalDate.format(date:string, format:string)`
* `LocalDate.addDays(date:string, amount:int)`
* `LocalDate.addWeeks(date:string, amount:int)`
* `LocalDate.addMonths(date:string, amount:int)`
* `LocalDate.addYears(date:string, amount:int)`

### `LocalDateTime`
* `LocalDateTime.parse(string)` or `LocalDateTime.valueOf(string)`
* `LocalDateTime.ofEpochSeconds(int)`
* `LocalDateTime.format(datetime:string, format:string)`
* `LocalDateTime.add(datetime:string, duration:string)`
* `LocalDateTime.subtract(datetime:string, duration:string)`
* `LocalDateTime.addSeconds(datetime:string, amount:int)`
* `LocalDateTime.addMinutes(datetime:string, amount:int)`
* `LocalDateTime.addHours(datetime:string, amount:int)`
* `LocalDateTime.addDays(datetime:string, amount:int)`
* `LocalDateTime.addWeeks(datetime:string, amount:int)`
* `LocalDateTime.addMonths(datetime:string, amount:int)`
* `LocalDateTime.addYears(datetime:string, amount:int)`


### `Timestamp`
* `Timestamp.parse(string)` or `Timestamp.valueOf(string)`
* `Timestamp.ofEpochSeconds(int)`

### `Duration`
* `Duration.ofDays(numberOfDays:int)`
* `Duration.ofHours(numberOfHours:int)`
* `Duration.ofMinutes(numberOfMinutes:int)`
* `Duration.ofSeconds(numberOfSeconds:int)`
* `Duration.ofMillis(numberOfMilliseconds:int)`
* `Duration.between(datetime:string,datetime:string)`
* `Duration.parse(string)` or `Duration.valueOf(string)`

### `Period`
* `Period.ofYears(numberOfYears:int)`
* `Period.ofMonths(numberOfMonths:int)`
* `Period.ofWeeks(numberOfWeeks:int)`
* `Period.ofDays(numberOfDays:int)`
* `Period.parse(string)` or `Period.valueOf(string)`

### `System`
* `System.getenv(env:string)` or `System.getenv(env:string, default:string)`
* `System.getProperty(env:string)` or `System.getProperty(env:string, default:string)`

### `String`
* `String.concat(left:string, right:string)`


### `AzureKeyVault` (since Flowman 1.1.0)
Provided by the [Azure plugin](../plugins/azure.md)
* `AzureKeyVault.getSecret(vaultName:string, secretName:string)` or `AzureKeyVault.getSecret(vaultName:string, secretName:string, linkedService:string)` 
