<template>
  <v-container
    fluid
  >
    <v-card>
      <v-row>
        <v-col cols="3">
          <project-selector/>
        </v-col>
        <v-col cols="3">
          <job-selector/>
        </v-col>
        <v-col cols="3">
          <phase-selector/>
        </v-col>
        <v-col cols="3">
          <status-selector/>
        </v-col>
      </v-row>
    </v-card>
    <v-card>
      <job-charts/>
    </v-card>
    <v-card>
      <v-card-title>Job History</v-card-title>
      <v-data-table
        dense
        :headers="headers"
        :items="jobs"
        :loading="loading"
        :expanded.sync="expanded"
        show-expand
        item-key="id"
        class="elevation-1"
        @click:row="clickRow"
      >
        <template v-slot:item.status="{ item }">
          <v-icon>
            {{ getIcon(item.status) }}
          </v-icon>
          {{ item.status }}
        </template>
        <template v-slot:expanded-item="{ item,headers }">
          <td :colspan="headers.length">
            <job-history-details :job="item.id"/>
          </td>
        </template>
      </v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  import JobHistoryDetails from "@/components/JobHistoryDetails";
  import JobCharts from "@/components/JobCharts";
  import ProjectSelector from "@/components/ProjectSelector";
  import JobSelector from "@/components/JobSelector";
  import PhaseSelector from "@/components/PhaseSelector";
  import StatusSelector from "@/components/StatusSelector";

  export default {
    components: {JobCharts, JobHistoryDetails, ProjectSelector, JobSelector, PhaseSelector, StatusSelector},
    props: {
    },

    data: () => ({
      jobs: [],
      expanded: [],
      total: 0,
      loading: false,
      headers: [
        { text: 'Job Run ID', value: 'id' },
        { text: 'Project Name', value: 'project' },
        { text: 'Project Version', value: 'version' },
        { text: 'Job Name', value: 'job' },
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
      clickRow(item, event) {
        if(event.isExpanded) {
          const index = this.expanded.findIndex(i => i === item);
          this.expanded.splice(index, 1)
        } else {
          this.expanded.push(item);
        }
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
      },

      getData() {
        this.loading = true
        this.$api.getJobsHistory()
          .then(response => {
            this.title = "Job History"
            this.jobs = response.data
            this.total = response.total
            this.expanded = []
            this.loading = false
          })
      }
    }
  }
</script>

<style>

</style>
