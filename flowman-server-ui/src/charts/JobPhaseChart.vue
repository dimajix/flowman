<template>
  <v-container fluid>
    <v-subheader class="title" inset>Phases</v-subheader>
    <pie-chart
      height="160"
      v-if="loaded"
      :chart-data="phases">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Filter from "@/mixins/Filter.js";

export default {
  name: 'JobPhaseChart',
  mixins: [Filter],
  components: { PieChart },

  data() {
    return {
      loaded: false,
      phases: {},
    };
  },

  methods: {
    getData() {
      this.$api.getJobCounts('phase', this.filter.projects, this.filter.jobs, this.filter.phases, this.filter.status)
        .then(response => {
          this.phases = {
            labels: ["Validate", "Create", "Build", "Verify", "Truncate", "Destroy"],
            datasets: [
              {
                backgroundColor: ["#ffd734", "#96be4f", "#41B883", "#00D8FF", "#b45b93", "#E42651"],
                data: [
                  response.data["VALIDATE"],
                  response.data["CREATE"],
                  response.data["BUILD"],
                  response.data["VERIFY"],
                  response.data["TRUNCATE"],
                  response.data["DESTROY"]
                ]
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
