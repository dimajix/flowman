<template>
  <v-container fluid>
      <v-subheader class="title" inset>Job Status</v-subheader>
      <pie-chart
        height="160"
        v-if="statusLoaded"
        :data="statusData">
      </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'JobStatusChart',
  components: { PieChart },

  props: {
    projectFilter:  { type: Array },
    jobFilter: { type: Array },
    statusFilter: { type: Array },
    phaseFilter: { type: Array },
  },

  data() {
    return {
      statusData: {},
      statusLoaded: false,
    };
  },

  watch: {
    projectFilter: function () { this.getData() },
    jobFilter: function () { this.getData() },
    statusFilter: function () { this.getData() },
    phaseFilter: function () { this.getData() },
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.statusLoaded = false
      this.$api.getJobCounts('status', this.projectFilter, this.jobFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          this.statusData = {
            labels: ["Success", "Skipped", "Failed"],
            datasets: [
              {
                backgroundColor: ["#41B883", "#00D8FF", "#E42651"],
                data: [response.data["SUCCESS"],  response.data["SKIPPED"], response.data["FAILED"]]
              }
            ]
          }
          this.statusLoaded = true
        })
    }
  }
};
</script>

<style>
</style>
