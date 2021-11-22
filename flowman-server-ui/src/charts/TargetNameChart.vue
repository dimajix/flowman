<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Name</v-subheader>
    <pie-chart
      height=160
      v-if="loaded"
      :chart-data="targets">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";
import Filter from "@/charts/Filter";

export default {
  name: 'TargetNameChart',
  mixins: [Filter],
  components: { PieChart },

  data() {
    return {
      loaded: false,
      targets: {},
    };
  },

  methods: {
    getData() {
      this.$api.getTargetCounts('target', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          const colorGradient = new Gradient();
          colorGradient.setGradient("#404060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.targets = {
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
