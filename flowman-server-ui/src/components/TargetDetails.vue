<template>
  <v-container fluid>
    <v-card-title>
      <v-icon>gavel</v-icon>
      Target '{{details.project}}/{{details.name}}' {{ details.phase }} id {{target}} status {{details.status}}
    </v-card-title>
    <v-card-text>
      {{graph}}
    </v-card-text>
  </v-container>
</template>

<script>
export default {
  name: 'TargetDetails',
  components: {},

  props: {
    target: String
  },

  data () {
    return {
      details: {},
      graph: {},
    }
  },

  mounted() {
    this.refresh()
  },

  methods: {
    refresh() {
      this.$api.getTargetDetails(this.target).then(response => {
        this.details = response
      })
      this.$api.getTargetGraph(this.target).then(response => {
        this.graph = response
      })
    }
  }
}
</script>
