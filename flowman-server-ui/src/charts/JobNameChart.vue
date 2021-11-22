<template>
  <v-container fluid>
    <v-subheader class="title" inset>Job Name</v-subheader>
    <pie-chart
      height="160"
      v-if="jobLoaded"
      :data="jobData">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";

export default {
  name: 'JobNameChart',
  components: { PieChart },

  props: {
    projectFilter:  { type: Array },
    jobFilter: { type: Array },
    statusFilter: { type: Array },
    phaseFilter: { type: Array },
  },

  data() {
    return {
      jobData: {},
      jobLoaded: false
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
      console.log("New Data!")
      this.jobLoaded = false
      this.$api.getJobCounts('job', this.projectFilter, this.jobFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          const colorGradient = new Gradient();
          colorGradient.setGradient("#404060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.jobData = {
            labels: Object.keys(response.data),
            datasets: [
              {
                backgroundColor: colorGradient.getArray(),
                data: Object.values(response.data)
              }
            ]
          }
          this.jobLoaded = true
        })
    }
  }
};
</script>

<style>
</style>
