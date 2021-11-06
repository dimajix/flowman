<template>
  <v-container
    fluid
  >
    <v-card>
      <v-card-title>Job History</v-card-title>
      <v-data-table
        dense
        :headers="headers"
        :items="jobs"
        :items-per-page="25"
        :loading="loading"
        item-key="id"
        class="elevation-1"
      ></v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  export default {
    props: {
      projectName: {
      }
    },

    data: () => ({
      jobs: [],
      loading: false,
      headers: [
        { text: 'Run ID', value: 'id' },
        { text: 'Project Name', value: 'project' },
        { text: 'Job Name', value: 'job' },
        { text: 'Build Phase', value: 'phase' },
        { text: 'Status', value: 'status' },
        { text: 'Started at', value: 'startDateTime' },
        { text: 'Finished at', value: 'endDateTime' },
      ]
    }),

    mounted() {
      this.getData()
    },

    methods: {
      getData() {
        this.loading = true
        this.$api.getAllJobsHistory()
          .then(response => {
            this.title = "Job History"
            this.jobs = response
            this.loading = false
          })
      }
    }
  }
</script>

<style>

</style>
