<template>
  <v-container>
    <v-layout
      text-xs-center
      wrap
    >
      <v-flex mb-4>
        <h1 class="display-2 font-weight-bold mb-3">
          Project Info for {{ projectName }}
        </h1>
        <p>Project Version: {{ version }}</p>
        <p>Project Description: {{ description }}</p>
        <p>Jobs: {{ jobs }}</p>
        <p>Targets: {{ jobs }}</p>
      </v-flex>
    </v-layout>
  </v-container>
</template>

<script>
  export default {
    props: {
      projectName: {
      }
    },

    data: () => ({
      name: "",
      version: "",
      description: null,
      jobs: [],
      targets: []
    }),

    watch: {
      '$route' () {
        this.$api.getProject(this.projectName)
          .then(response => {
            this.name = response.name
            this.version = response.version
            this.jobs = response.jobs
            this.targets = response.targets
          })
      }
    }
  }
</script>

<style>

</style>
