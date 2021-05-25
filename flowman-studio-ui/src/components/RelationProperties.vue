<template>
  <v-container>
    <v-card>
      <v-card-title><v-icon>table_view</v-icon>Relation '{{relation}}'</v-card-title>
      <v-simple-table>
        <template v-slot:default>
          <tbody>
          <tr>
            <td>Name</td>
            <td>{{ properties.name }}</td>
          </tr>
          <tr>
            <td>Kind</td>
            <td>{{ properties.kind }}</td>
          </tr>
          </tbody>
        </template>
      </v-simple-table>
    </v-card>
  </v-container>
</template>

<script>
export default {
  name: 'RelationProperties',

  props: {
    relation: String
  },

  data () {
    return {
      properties: {}
    }
  },

  mounted() {
    this.refresh()
  },

  watch: {
    relation: function() {
      this.refresh()
    },
  },

  methods: {
    refresh() {
      this.$api.getRelation(this.relation).then(response => {
        this.properties = {
          name: response.name,
          kind: response.kind,
        }
      })
    }
  },

}
</script>
