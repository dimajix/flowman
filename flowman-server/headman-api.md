/api/v1/projects    
    GET - List all projects

/api/v1/projects/<project>    
    PUT - Create or Update
    DELETE - Delete
    GET - Get information


/api/v1/projects/<project>/config
    GET - List all configs
    
/api/v1/projects/<project>/config/<config>
    PUT - Create or Update
    DELETE - Delete
    GET - Get information
    

/api/v1/projects/<project>/mappings    
    GET - List all mappings

/api/v1/projects/<project>/mappings/<mapping>
    PUT - Create or Update
    DELETE - Delete
    GET - Get information (describe)

/api/v1/projects/<project>/mappings/<mapping>/plan
    GET - Get execution plan

/api/v1/projects/<project>/mappings/<mapping>/schema
    GET - Get schema

/api/v1/projects/<project>/mappings/<mapping>/status
    GET - Validate

/api/v1/projects/<project>/mappings/<mapping>/records
    GET - Dump records
    

/api/v1/projects/<project>/outputs    
    GET - List all outputs

/api/v1/projects/<project>/outputs/<output>    
    PUT - Create or Update
    DELETE - Delete
    GET - Get information (describe)

/api/v1/projects/<project>/outputs/<output>/run
    POST - Run output

/api/v1/projects/<project>/outputs/<output>/status
    GET - Validate


/api/v1/projects/<project>/jobs
    GET - List all jobs

/api/v1/projects/<project>/jobs/<job>
    PUT - Create or Update
    DELETE - Delete
    GET - Get information (describe)

/api/v1/projects/<project>/jobs/<job>/run
    POST - Run job


/api/v1/projects/<project>/models
    GET - List all models
    
/api/v1/projects/<project>/models/<model>
    PUT - Create or Update
    DELETE - Delete
    GET - Get information (describe)

/api/v1/projects/<project>/models/<model>/instance
    PUT - Instantiate model
    PATCH - Migrate model
    DELETE - Delete model instance

/api/v1/projects/<project>/models/<model>/records
    GET - Dump records
