<template>
  <v-container>
    <v-card>
      <v-card-title><v-icon>mediation</v-icon>Mapping '{{mapping}}'</v-card-title>
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
          <tr>
            <td>Inputs</td>
            <td>{{ properties.inputs }}</td>
          </tr>
          <tr>
            <td>Outputs</td>
            <td>{{ properties.outputs }}</td>
          </tr>
          <tr>
            <td>Broadcast</td>
            <td>{{ properties.broadcast }}</td>
          </tr>
          <tr>
            <td>Cache</td>
            <td>{{ properties.cache }}</td>
          </tr>
          </tbody>
        </template>
      </v-simple-table>
    </v-card>
  </v-container>
</template>

<script>
export default {
  name: 'MappingProperties',

  props: {
    mapping: String
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
    mapping: function() {
      this.refresh()
    },
  },

  methods: {
    refresh() {
      this.$api.getMapping(this.mapping).then(response => {
        this.properties = {
          name: response.name,
          kind: response.kind,
          inputs: response.inputs,
          outputs: response.outputs,
          broadcast: response.broadcast,
          cache: response.cache
        }
      })
    }
  },

}
</script>
