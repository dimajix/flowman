<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Status</v-subheader>
    <pie-chart
      height=160
      v-if="loaded"
      :chart-data="status">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Filter from "@/charts/Filter";

export default {
  name: 'TargetStatusChart',
  mixins: [Filter],
  components: { PieChart },

  data() {
    return {
      status: {},
      loaded: false,
    };
  },

  methods: {
    getData() {
      this.$api.getTargetCounts('status', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
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
