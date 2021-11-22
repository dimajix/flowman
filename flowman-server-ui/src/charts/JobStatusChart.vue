<template>
  <v-container fluid>
      <v-subheader class="title" inset>Job Status</v-subheader>
      <pie-chart
        height="160"
        v-if="loaded"
        :chart-data="status">
      </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Filter from "@/mixins/Filter.js";

export default {
  name: 'JobStatusChart',
  mixins: [Filter],
  components: { PieChart },

  data() {
    return {
      loaded: false,
      status: {},
    };
  },

  methods: {
    getData() {
      this.$api.getJobCounts('status', this.filter.projects, this.filter.jobs, this.filter.phases, this.filter.status)
        .then(response => {
          this.status = {
            labels: ["Success", "Skipped", "Failed"],
            datasets: [
              {
                backgroundColor: ["#41B883", "#00D8FF", "#E42651"],
                data: [response.data["SUCCESS"],  response.data["SKIPPED"], response.data["FAILED"]]
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
