<template>
  <v-container fluid>
    <v-row>
      <v-col cols="12">
        <v-card outlined elevation="2">
          <v-card-text>
            <v-row justify="space-around">
              <v-col cols="3">
                <v-card-subtitle class="text-h6">Project</v-card-subtitle>
                <v-select
                  v-model="selectedProject"
                  :items="projects"
                  solo
                  label="Project Name"
                  append-icon="expand_more"
                ></v-select>
              </v-col>
              <v-col cols="3">
                <v-card-subtitle class="text-h6">Job</v-card-subtitle>
                <v-select
                  v-model="selectedJob"
                  :items="jobs"
                  solo
                  label="Job Name"
                  append-icon="expand_more"
                ></v-select>
              </v-col>
              <v-col cols="3">
                <v-card-subtitle class="text-h6">Execution Phase</v-card-subtitle>
                <v-select
                  v-model="selectedPhase"
                  :items="['VALIDATE', 'CREATE', 'BUILD', 'VERIFY', 'TRUNCATE', 'DESTROY']"
                  solo
                  append-icon="expand_more"
                  label="Execution Phase"
                ></v-select>
              </v-col>
              <v-col cols="3">
                <v-card-subtitle class="text-h6">Execution Status</v-card-subtitle>
                <v-select
                  v-model="selectedStatus"
                  :items="['SUCCESS', 'SUCCESS_WITH_ERRORS', 'SKIPPED', 'FAILED', 'RUNNING']"
                  solo
                  multiple
                  chips
                  clearable
                  deletable-chips
                  append-icon="expand_more"
                  clear-icon="clear"
                  label="Filter by Execution Status"
                ></v-select>
              </v-col>
            </v-row>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-row
    >
      <v-col
        v-for="item in metrics"
        :key="item.id"
        cols="6"
      >
        <v-card>
          <v-card-title>Project '{{item.project}}' job '{{item.job}}' metric '{{item.metric}}'</v-card-title>
          <v-card-subtitle>{{item.phase}} {{item.labels.category}} of kind '{{item.labels.kind}}' with name '{{item.labels.name}}'</v-card-subtitle>
          <v-card-text>
            <v-sparkline
              height="30"
              fill
              smooth="8"
              :value="Array.from(item.measurements).map(m => m.value)"
              :labels="Array.from(item.measurements).map(m => m.value)"
              label-size="4.0"
              line-width="0.4"
              padding="6"
            ></v-sparkline>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>

export default {
  name: 'Metrics',

  data() {
    return {
      loading: false,
      projects: [],
      jobs: [],
      selectedProject: "",
      selectedJob: "",
      selectedPhase: "BUILD",
      selectedStatus: [],
      metrics: []
    };
  },

  watch: {
    selectedProject: function() {
      this.getJobList()
      this.getMetricData()
    },
    selectedJob: function() {
      this.getMetricData()
    },
    selectedPhase: function() {
      this.getMetricData()
    },
    selectedStatus: function() {
      this.getMetricData()
    },
  },

  mounted() {
    this.getProjectList()
    this.getJobList()
    //this.getMetricData()
  },

  methods: {
    getProjectList() {
      this.$api.getJobCounts('project')
        .then(response => {
          this.projects =  Object.keys(response.data)
          this.selectedProject = this.projects[0]
        })
    },
    getJobList() {
      this.$api.getJobCounts('job')
        .then(response => {
          this.jobs =  Object.keys(response.data)
          this.selectedJob = this.jobs[0]
        })
    },
    getMetricData() {
      this.loading = true
      this.$api.getJobsMetrics([this.selectedProject], [this.selectedJob], [this.selectedPhase], this.selectedStatus, ['category','kind','name'])
        .then(response => {
          this.metrics = response.data
          this.loading = false
        })
    }

  }
}
</script>
