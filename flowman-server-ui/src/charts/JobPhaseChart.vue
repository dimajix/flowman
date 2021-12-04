<template>
  <v-container fluid>
    <pie-chart
      :height="height"
      v-if="loaded"
      :chart-data="phases"
      :title-position="titlePosition"
    >
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

  props: {
    titlePosition: {
      type: String,
      default: () => 'top'
    },
    height: {
      type: Number,
      default: () => 180
    }
  },

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
            title: "Phases",
            labels: ["Validate", "Create", "Build", "Verify", "Truncate", "Destroy"],
            datasets: [
              {
                backgroundColor: ["#d0d752", "#96be4f", "#41B883", "#00D8FF", "#b45b93", "#E42651"],
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
