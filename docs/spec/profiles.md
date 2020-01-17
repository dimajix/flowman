
# Flowman Profiles

Flowman supports so called *profiles* which contain again some specifications like environment
variables, Spark configurations and connections. But in contrast to a module definition, these
entities are not used per default, instead a profile needs to bew *activated* on the command
line.

Using profiles allows to define environment-specific settings (like for the test and for the
prod environment, which may have different database names, host names, credentials and so on).
Any active profile will override any entity within a project with the same name (for example
a profiles Spark configuration properties will override the ones in a modules `config`
section)

## Defining Profiles

TBD

## Activating Profiles

TBD
