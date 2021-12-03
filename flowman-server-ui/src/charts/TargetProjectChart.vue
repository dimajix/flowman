<template>
  <v-container fluid>
    <v-subheader class="title" inset>Projects</v-subheader>
    <pie-chart
      height=160
      v-if="loaded"
      :chart-data="projects">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";
import Filter from "@/mixins/Filter.js";

export default {
  name: 'TargetProjectChart',
  mixins: [Filter],
  components: {
    PieChart
  },

  data() {
    return {
      projects: {},
      loaded: false,
    };
  },

  methods: {
    getData() {
      this.$api.getTargetCounts('project', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          const colorGradient = new Gradient();
          colorGradient.setGradient("#407060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.projects = {
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
