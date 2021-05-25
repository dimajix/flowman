<template>
  <v-container>
    <v-card>
      <v-card-title><v-icon>gavel</v-icon>Job '{{job}}'</v-card-title>
      <v-simple-table>
        <template v-slot:default>
          <tbody>
          <tr>
            <td>Name</td>
            <td>{{ properties.name }}</td>
          </tr>
          <tr>
            <td>Description</td>
            <td>{{ properties.description }}</td>
          </tr>
          <tr>
            <td>Parameters</td>
            <td>{{ properties.parameters }}</td>
          </tr>
          <tr>
            <td>Targets</td>
            <td>{{ properties.targets }}</td>
          </tr>
          </tbody>
        </template>
      </v-simple-table>
    </v-card>
  </v-container>
</template>

<script>
export default {
  name: 'JobProperties',

  props: {
    job: String
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
    job: function() {
      this.refresh()
    },
  },

  methods: {
    refresh() {
      this.$api.getJob(this.job).then(response => {
        this.properties = {
          name: response.name,
          description: response.description,
          parameters: response.parameters,
          targets: response.targets,
        }
      })
    }
  },

}
</script>
