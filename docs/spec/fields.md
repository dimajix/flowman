---
layout: page
title: Flowman Fields & Values
permalink: /spec/fields.html
---
# Fields & Values

## Specifying Fields
```
name: id
type: String
nullable: false
description: "This is the primary ID"
default:
size:
format:
```

## Specifying Partition Columns
```
name: id
type: Integer
granularity: 1
description: "This is the primary ID"
```

## Specifying Values

### Single Values
```
variable: value
```

### Array Values
```
variable: 
 - value_1
 - value_2
```


### Range Values
```
variable:
  start: 1 
  end: 10
  step: 3 
```
