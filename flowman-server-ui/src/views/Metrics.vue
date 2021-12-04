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
          <v-card-text class="pa-2 ma-0">
            <apexchart
              type="bar"
              height="240"
              :options="getApexOptions(item)"
              :series="getApexSeries(item)">
            </apexchart>
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
      metrics: [],
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
    getApexSeries(item) {
      return [{
        name: item.metric,
        data: item.measurements.map(i => i.value)
      }]
    },
    getApexOptions(item) {
      return {
        title: {
          text: item.metric + ' of ' + item.labels.category + " '"  +item.labels.kind + "/" + item.labels.name + "'",
          style: {
            fontSize: '18px',
          },
        },
        subtitle: {
          text: item.phase + " project '" + item.project + "' job '" + item.job + "'",
        },
        chart: {
          id: item.metric
        },
        tooltip: {
          x: {
            // eslint-disable-next-line no-unused-vars
            formatter: function(value, {series, seriesIndex, dataPointIndex, w}) {
              let labels = Object.entries(item.measurements[dataPointIndex].labels).map(l => '<tr><td>' + l[0] + '</td><td>' + l[1] + '</td></tr>').reduce((l,r) => l + r, '')
              return '<div class="subtitle-1 justify-center">' + value + '</div>' + '<div><table><tbody>' + labels + '</tbody></table></div>'
            }
          }
        },
        xaxis: {
          categories: item.measurements.map(i => i.ts),
          labels: {
            show: true,
            rotate: -45,
            rotateAlways: false,
            hideOverlappingLabels: true,
            showDuplicates: false,
            trim: true,
            minHeight: undefined,
            maxHeight: 60,
            offsetX: 0,
            offsetY: 0,
            datetimeUTC: true,
            datetimeFormatter: {
              year: 'yyyy',
              month: "MMM 'yy",
              day: 'dd MMM',
              hour: 'HH:mm',
            },
          },
        },
      }
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
