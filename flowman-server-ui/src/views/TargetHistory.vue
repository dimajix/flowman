<template>
  <v-container
    fluid
  >
    <v-card>
      <v-row>
        <v-col cols="3">
          <project-selector/>
        </v-col>
        <v-col cols="2">
          <job-selector/>
        </v-col>
        <v-col cols="3">
          <target-selector/>
        </v-col>
        <v-col cols="2">
          <phase-selector/>
        </v-col>
        <v-col cols="2">
          <status-selector/>
        </v-col>
      </v-row>
    </v-card>
    <v-card>
      <target-charts/>
    </v-card>
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
  import TargetCharts from "@/components/TargetCharts";
  import ProjectSelector from "@/components/ProjectSelector";
  import JobSelector from "@/components/JobSelector";
  import PhaseSelector from "@/components/PhaseSelector";
  import StatusSelector from "@/components/StatusSelector";
  import TargetSelector from "@/components/TargetSelector";

  export default {
    components: {TargetCharts, ProjectSelector, JobSelector, PhaseSelector, StatusSelector, TargetSelector},
    props: {
    },

    data: () => ({
      targets: [],
      total: 0,
      loading: false,
      headers: [
        { text: 'Target Run ID', value: 'id' },
        { text: 'Job Run ID', value: 'jobId' },
        { text: 'Project Name', value: 'project' },
        { text: 'Project Version', value: 'version' },
        { text: 'Target Name', value: 'target' },
        { text: 'Build Phase', value: 'phase' },
        { text: 'Status', value: 'status' },
        { text: 'Started at', value: 'startDateTime' },
        { text: 'Finished at', value: 'endDateTime' },
        { text: 'Error message', value: 'error' },
      ]
    }),

    mounted() {
      this.getData()
    },

    methods: {
      getData() {
        this.loading = true
        this.$api.getTargetsHistory()
          .then(response => {
            this.targets = response.data
            this.total = response.total
            this.loading = false
          })
      }
    }
  }
</script>

<style>

</style>
