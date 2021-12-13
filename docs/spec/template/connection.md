# Connection Template

## Example
```yaml
# All template definitions (independent of their kind) go into the templates section
templates:
  default_connection:
    kind: connection
    parameters:
      - name: dir
        type: string
    template:
      # This is now a normal connection definition, which can also access the parameters as variables 
      kind: jdbc
      driver: "org.apache.derby.jdbc.EmbeddedDriver"
      url: $String.concat('jdbc:derby:', $dir, '/logdb;create=true')
      username: "some_user"
      password: "secret"

# Now you can create instances of the template in the corresponding entity section or at any other place where
# a connection is allowed
connections:
  frontend:
    kind: template/default_connection
    dir: /opt/flowman/derby
    
relations:
  rel_1:
    kind: jdbc
    connection:
      kind: template/default_connection
      dir: /opt/flowman/derby_new
    table: "advertiser_setting"
    schema:
        kind: embedded
        fields:
            - name: id
              type: Integer
            - name: business_rule_id
              type: Integer
            - name: rtb_advertiser_id
              type: Integer

  rel_2:
    kind: jdbc
    connection: frontend
    query: "
      SELECT
        *
      FROM line_item li
    "

```
