<template>
  <v-container fluid>
    <v-subheader class="title" inset>Jobs</v-subheader>
    <pie-chart
      height="160"
      v-if="loaded"
      :chart-data="jobs">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Filter from "@/mixins/Filter.js";
import Gradient from "javascript-color-gradient";

export default {
  mixins: [Filter],
  name: 'JobNameChart',
  components: { PieChart },

  data() {
    return {
      loaded: false,
      jobs: {},
    };
  },

  methods: {
    getData() {
      this.$api.getJobCounts('job', this.filter.projects, this.filter.jobs, this.filter.phases, this.filter.status)
        .then(response => {
          const colorGradient = new Gradient();
          colorGradient.setGradient("#404060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.jobs = {
            labels: Object.keys(response.data),
            datasets: [
              {
                backgroundColor: colorGradient.getArray(),
                data: Object.values(response.data)
              }
            ]
          }
          this.loaded = true
        })
    }
  }
};
</script>

<style>
</style>
