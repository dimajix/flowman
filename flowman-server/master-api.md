
/api/v1/namespaces
    GET - List all namespaces

/api/v1/namespaces/<namespace>
    PUT - Create or Update
    DELETE - Delete
    GET - Get information

    
/api/v1/namespaces/<namespace>/config
    GET - List all configs

/api/v1/namespaces/<namespace>/config/<config>
    PUT - Create or Update
    DELETE - Delete
    GET - Get information


/api/v1/namespaces/<namespace>/headman    
    GET - Retrieve status
    POST - (Re)start headman
    DELETE - Stop headman
    
    
/api/v1/namespaces/<namespace>/projects    
    * - Delegate to Headman
