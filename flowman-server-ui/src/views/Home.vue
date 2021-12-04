<template>
  <v-container
    fluid
  >
    <v-row dense>
      <v-col cols="6">
        <v-card shaped outlined elevation="2">
          <v-card-title>Jobs</v-card-title>
          <v-row dense>
            <v-col cols="6">
              <job-status-chart title-position="chartArea" height="160"/>
            </v-col>
            <v-col cols="6">
              <job-phase-chart title-position="chartArea" height="160"/>
            </v-col>
          </v-row>
        </v-card>
      </v-col>

      <v-col cols="6">
        <v-card shaped outlined elevation="2">
          <v-card-title>Targets</v-card-title>
          <v-row>
            <v-col cols="6">
              <target-status-chart title-position="chartArea" height="160"/>
            </v-col>
            <v-col cols="6">
              <target-phase-chart title-position="chartArea" height="160"/>
            </v-col>
          </v-row>
        </v-card>
      </v-col>
    </v-row>

    <v-row>
      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <v-card-title>Last 5 Jobs</v-card-title>
          <v-data-table
            :headers="jobHeaders"
            :items="jobs"
            :loading="jobsLoading"
            item-key="id"
            class="elevation-1"
            hide-default-footer
          >
            <template v-slot:item.args="{ item }">
              <v-chip
                v-for="p in Object.entries(item.args) "
                :key="p[0]"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </template>
            <template v-slot:item.status="{ item }">
              <status :status="item.status"/>
            </template>
            <template v-slot:expanded-item="{ item,headers }">
              <td :colspan="headers.length">
                <job-history-details :job="item.id"/>
              </td>
            </template>
          </v-data-table>
        </v-card>
      </v-col>
    </v-row>

    <v-row>
     <v-col cols="12">
        <v-card shaped>
          <v-card-title>Last 5 Targets</v-card-title>
          <v-data-table
            :headers="targetHeaders"
            :items="targets"
            :loading="targetsLoading"
            item-key="id"
            class="elevation-1"
            hide-default-footer
          >
            <template v-slot:item.status="{ item }">
              <status :status="item.status"/>
            </template>
            <template v-slot:item.partitions="{ item }">
              <v-chip
                v-for="p in Object.entries(item.partitions) "
                :key="p[0]"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </template>
          </v-data-table>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
  import TargetStatusChart from "@/charts/TargetStatusChart";
  import TargetPhaseChart from "@/charts/TargetPhaseChart";
  import JobStatusChart from "@/charts/JobStatusChart";
  import JobPhaseChart from "@/charts/JobPhaseChart";
  import Status from '@/components/Status.vue'

  export default {
    components: {
      JobPhaseChart,
      JobStatusChart,
      TargetStatusChart,
      TargetPhaseChart,
      Status,
    },

    data() {
      return {
        targetsLoading: false,
        targets: [],
        jobsLoading: false,
        jobs: [],
        targetHeaders: [
          {text: 'Project Name', value: 'project'},
          {text: 'Target Name', value: 'target'},
          {text: 'Partition', value: 'partitions'},
          {text: 'Build Phase', value: 'phase'},
          {text: 'Status', value: 'status', width:120},
          {text: 'Started at', value: 'startDateTime'},
          {text: 'Finished at', value: 'endDateTime'},
          {text: 'Error message', value: 'error', width:160},
        ],
        jobHeaders: [
          { text: 'Project Name', value: 'project' },
          { text: 'Job Name', value: 'job' },
          { text: 'Parameters', value: 'args' },
          { text: 'Build Phase', value: 'phase' },
          { text: 'Status', value: 'status', width:120 },
          { text: 'Started at', value: 'startDateTime' },
          { text: 'Finished at', value: 'endDateTime' },
          { text: 'Error message', value: 'error', width:160 },
        ]
      }
    },

    mounted() {
      this.getData()
    },

    methods: {
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
        this.jobsLoading = true
        this.targetsLoading = true
        this.$api.getJobsHistory([], [], [], [], 0, 5)
          .then(response => {
            this.jobs = response.data
            this.jobsLoading = false
          })

        this.$api.getTargetsHistory([], [], [], [], [], 0, 5)
          .then(response => {
            this.targets = response.data
            this.targetsLoading = false
          })
      }
    }
  }
</script>


<style>
.side-tab {
  position: fixed;
  justify-content: center;
  justify-self: center;
  width: available;
}
</style>
