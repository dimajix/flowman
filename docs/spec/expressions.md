# Script Expressions

Flowman allows using *expressions* at many places in the YAML specification files. For example, you can reference
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
This example uses a variable `weather_source` to define which relation to read from. This way,
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
* `Integer.parse(string) -> int` 
* `Integer.valueOf(string) -> int`

### `Float`
* `Float.parse(string) -> float`
* `Float.valueOf(string) -> float`

### `Boolean`
* `Boolean.parse(string) -> boolean`
* `Boolean.valueOf(string) -> boolean`

### `LocalDate`
* `LocalDate.now() -> LocalDate`
* `LocalDate.today() -> LocalDate`
* `LocalDate.parse(date:string) -> LocalDate`
* `LocalDateTime.valueOf(date:string) -> LocalDate`
* `LocalDate.format(date:string, format:string) -> string`
* `LocalDate.addDays(date:string, days:int) -> LocalDate`
* `LocalDate.addWeeks(date:string, weeks:int) -> LocalDate`
* `LocalDate.addMonths(date:string, months:int) -> LocalDate`
* `LocalDate.addYears(date:string, years:int) -> LocalDate`

### `LocalDateTime`
* `LocalDateTime.now() -> LocalDateTime`
* `LocalDateTime.parse(datetime:string) -> LocalDateTime`
* `LocalDateTime.valueOf(datetime:string) -> LocalDateTime`
* `LocalDateTime.ofEpochSeconds(epoch:int) -> LocalDateTime`
* `LocalDateTime.format(datetime:string, format:string) -> string`
* `LocalDateTime.add(datetime:string, duration:string) -> LocalDateTime`
* `LocalDateTime.subtract(datetime:string, duration:string) -> LocalDateTime`
* `LocalDateTime.addSeconds(datetime:string, seconds:int) -> LocalDateTime`
* `LocalDateTime.addMinutes(datetime:string, minutes:int) -> LocalDateTime`
* `LocalDateTime.addHours(datetime:string, hours:int) -> LocalDateTime`
* `LocalDateTime.addDays(datetime:string, days:int) -> LocalDateTime`
* `LocalDateTime.addWeeks(datetime:string, weeks:int) -> LocalDateTime`
* `LocalDateTime.addMonths(datetime:string, months:int) -> LocalDateTime`
* `LocalDateTime.addYears(datetime:string, years:int) -> LocalDateTime`

### `Timestamp`
* `Timestamp.parse(ts:string) -> Timestamp`
* `Timestamp.valueOf(ts:string) -> Timestamp`
* `Timestamp.ofEpochSeconds(epoch:int) -> Timestamp`
* `Timestamp.add(ts:string, duration:string) -> Timestamp`
* `Timestamp.subtract(ts:string, duration:string) -> Timestamp`

### `Duration`
* `Duration.ofDays(days:int) -> Duration`
* `Duration.ofHours(hours:int) -> Duration`
* `Duration.ofMinutes(minutes:int) -> Duration`
* `Duration.ofSeconds(seconds:int) -> Duration`
* `Duration.ofMillis(milliseconds:int) -> Duration`
* `Duration.between(datetime:string, datetime:string) -> Duration`
* `Duration.parse(duration:string) -> Duration`
* `Duration.valueOf(duration:string) -> Duration`

### `Period`
* `Period.ofYears(years:int) -> Period`
* `Period.ofMonths(months:int) -> Period`
* `Period.ofWeeks(weeks:int) -> Period`
* `Period.ofDays(days:int) -> Period`
* `Period.parse(period:string) -> Period`
* `Period.valueOf(period:string) -> Period`

### `System`
* `System.getenv(env:string) -> string`
* `System.getenv(env:string, default:string) -> string`
* `System.getProperty(property:string) -> string`
* `System.getProperty(property:string, default:string) -> string`

### `String`
* `String.concat(left:string, right:string) -> string`
* `String.concat(s1:string,s2:string, s3:string) -> string`
* `String.concat(s1:string,s2:string, s3:string, s4:string) -> string`
* `String.concat(s1:string,s2:string, s3:string, s4:string, s5:string) -> string`
* `String.urlEncode(url:string) -> string`
* `String.escapePathName(url:string) -> string`
* `String.partitionEncode(url:string) -> string`

### `URL`
* `URL.encode(url:string) -> string`
* `URL.decode(url:string) -> string`

### `File`
* `File.read(filename:string) -> string`

### `JSON`
* `JSON.path(json:string, path:string) -> string`

### `AzureKeyVault` (since Flowman 1.1.0)
The following functions are provided by the [Azure Plugin](../plugins/azure.md)
* `AzureKeyVault.getSecret(vaultName:string, secretName:string) -> string`
* `AzureKeyVault.getSecret(vaultName:string, secretName:string, linkedService:string) -> string` 

### `AwsSecretsManager` (since Flowman 1.1.0)
The following functions are provided by the [AWS Plugin](../plugins/aws.md)
* `AwsSecretsManager.getSecret(secretName:string, region:string) -> string`
* `AwsSecretsManager.getSecret(secretName:string, keyName:string, region:string) -> string`
