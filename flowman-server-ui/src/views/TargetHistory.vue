<template>
  <v-container
    fluid
  >
    <v-card>
      <target-charts
        :project-filter="projectFilter"
        :job-filter="jobFilter"
        :target-filter="targetFilter"
        :phase-filter="phaseFilter"
        :status-filter="statusFilter"
      />
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
      >
        <template v-slot:item.status="{ item }">
          <v-icon>
            {{ getIcon(item.status) }}
          </v-icon>
          {{ item.status }}
        </template>
      </v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  import TargetCharts from "@/components/TargetCharts";

  export default {
    components: {TargetCharts},
    props: {
    },

    data: () => ({
      targets: [],
      total: 0,
      loading: false,
      jobFilter: [],
      projectFilter: [],
      targetFilter: [],
      phaseFilter: [],
      statusFilter: [],
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
      },

      getIcon(status) {
        if (status === "SUCCESS") {
          return "done_all"
        }
        else if (status === "SKIPPED") {
          return "fast_forward"
        }
        else if (status === "FAILED") {
          return "error"
        }
        else {
          return "warning_amber"
        }
      }
    }
  }
</script>

<style>

</style>
