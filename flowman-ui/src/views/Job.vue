<template>
  <v-container>
    <v-layout
      text-xs-center
      wrap
    >
      <v-flex mb-4>
        <h1 class="display-2 font-weight-bold mb-3">
          Job Info
        </h1>
        <p>Job Name: {{ name }}</p>
        <p>Environment: {{ environment }}</p>
        <p>Environment: {{ parameters }}</p>
      </v-flex>
    </v-layout>
  </v-container>
</template>

<script>
export default {
  props: {
    projectName: {},
    jobName: {}
  },

  data: () => ({
    name: "",
    environment: [],
    parameters: []
  }),

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.loading = true
      this.$api.getJob(this.projectName, this.jobName)
        .then(response => {
          this.name = response.name
          this.environment = response.environment
          this.plugiparametersns = response.parameters
        })
    }
  }
}
</script>

<style>

</style>
