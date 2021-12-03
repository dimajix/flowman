<template>
  <v-container fluid>
    <v-subheader class="title" inset>Targets</v-subheader>
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
import Filter from "@/mixins/Filter.js";

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
          let entries = Object.entries(response.data).sort((l,r) => l[1] <= r[1])
          if (entries.length > 9) {
            let remaining = entries.slice(8)
            let remainingTotal = remaining.map(e => e[1]).reduce((a,v) => a+v, 0)
            let remainingCount = remaining.length
            entries = entries.slice(0, 8).concat([[remainingCount + " more targets...", remainingTotal]])
          }

          const colorGradient = new Gradient();
          colorGradient.setGradient("#404060", "#9090e0");
          colorGradient.setMidpoint(entries.length);

          this.targets = {
            labels: entries.map(e => e[0]),
            datasets: [
              {
                backgroundColor: colorGradient.getArray(),
                data: entries.map(e => e[1])
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
