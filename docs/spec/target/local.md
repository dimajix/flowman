# Flowman Local Target

## Fields
 * `kind` **(mandatory)** *(string)*: `local`
 * `mapping` **(mandatory)** *(string)*:
 Specifies the name of the input mapping to be counted
 * `filename` **(mandatory)** *(string)*:
 * `encoding` **(optional)** *(string)* *(default: "UTF-8")*: 
 * `header` **(optional)** *(boolean)* *(default: true)*: 
 * `newline` **(optional)** *(string)* *(default: "\n")*: 
 * `delimiter` **(optional)** *(string)* *(default: ",")*: 
 * `quote` **(optional)** *(string)* *(default: "\"")*: 
 * `escape` **(optional)** *(string)* *(default: "\\")*: 
 * `escape` **(optional)** *(list)* *(default: [])*: 


## Supported Phases
* `BUILD`
* `VERIFY`
* `TRUNCATE`
* `DESTROY`
