<template>
  <v-container fluid>
    <v-subheader class="title" inset>Job Name</v-subheader>
    <pie-chart
      height="160"
      v-if="jobLoaded"
      :data="jobData"
      :options="chartOptions">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";

export default {
  name: 'JobNameChart',
  components: { PieChart },

  data() {
    return {
      chartOptions: {
        responsive: true,
        maintainAspectRatio: true,
        hoverBorderWidth: 20,
        legend: {
          position: 'right',
          align: 'center'
        },
      },
      jobData: {},
      jobLoaded: false
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.jobLoaded = false
      this.$api.getJobCounts('job')
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
