<template>
  <v-container
    fluid
  >
    <v-card>
      <v-card-title>Target History</v-card-title>
      <v-data-table
        dense
        :headers="headers"
        :items="targets"
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
      targets: [],
      loading: false,
      headers: [
        { text: 'Run ID', value: 'id' },
        { text: 'Job Run ID', value: 'jobId' },
        { text: 'Project Name', value: 'project' },
        { text: 'Target Name', value: 'target' },
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
        this.$api.getAllTargetsHistory()
          .then(response => {
            this.targets = response
            this.loading = false
          })
      }
    }
  }
</script>

<style>

</style>
