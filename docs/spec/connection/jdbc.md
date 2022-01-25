# JDBC Connection

## Example
```yaml
environment:
  - mysql_db_driver=$System.getenv('MYSQL_DRIVER', 'com.mysql.cj.jdbc.Driver')
  - mysql_db_url=$System.getenv('MYSQL_URL')
  - mysql_db_username=$System.getenv('MYSQL_USERNAME')
  - mysql_db_password=$System.getenv('MYSQL_PASSWORD')

connections:
  mysql-db:
    kind: jdbc
    driver: "$mysql_db_driver"
    url: "$mysql_db_url"
    username: "$mysql_db_username"
    password: "$mysql_db_password"
```

## Fields
* `kind` **(mandatory)** *(string)*: `jdbc`

* `driver` **(mandatory)** *(string)*

* `url` **(mandatory)** *(string)*

* `username` **(optional)** *(string)*

* `password` **(optional)** *(string)*

* `properties` **(optional)** *(map)* 


## Description
