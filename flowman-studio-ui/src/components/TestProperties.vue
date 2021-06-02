<template>
  <v-container>
    <v-card>
      <v-card-title><v-icon>rule</v-icon>Test '{{test}}'</v-card-title>
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
          </tbody>
        </template>
      </v-simple-table>
    </v-card>
  </v-container>
</template>

<script>
export default {
  name: 'TestProperties',

  props: {
    test: String
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
    test: function() {
      this.refresh()
    },
  },

  methods: {
    refresh() {
      this.$api.getTest(this.test).then(response => {
        this.properties = {
          name: response.name,
          description: response.description,
          labels: response.labels,
        }
      })
    }
  },

}
</script>
