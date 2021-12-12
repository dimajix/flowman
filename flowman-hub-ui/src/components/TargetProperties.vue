<template>
  <v-container>
    <v-card>
      <v-card-title><v-icon>label</v-icon>Target '{{target}}'</v-card-title>
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
  name: 'TargetProperties',

  props: {
    target: String
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
    target: function() {
      this.refresh()
    },
  },

  methods: {
    refresh() {
      this.$api.getTarget(this.target).then(response => {
        this.properties = {
          name: response.name,
          kind: response.kind,
          before: response.before,
          after: response.after,
          labels: response.labels,
        }
      })
    }
  },

}
</script>
