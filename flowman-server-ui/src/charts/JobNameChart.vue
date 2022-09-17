<template>
  <v-container fluid>
    <pie-chart
      :height="height"
      v-if="loaded"
      :chart-data="jobs"
      :title-position="titlePosition"
    >
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
      jobs: {},
    };
  },

  methods: {
    getData() {
      this.$api.getJobCounts('job', this.filter.projects, this.filter.jobs, this.filter.phases, this.filter.status)
        .then(response => {
          let entries = Object.entries(response.data).sort((l,r) => l[1] <= r[1])
          if (entries.length > 9) {
            let remaining = entries.slice(8)
            let remainingTotal = remaining.map(e => e[1]).reduce((a,v) => a+v, 0)
            let remainingCount = remaining.length
            entries = entries.slice(0, 8).concat([[remainingCount + " more jobs...", remainingTotal]])
          }

          const colorGradient = new Gradient();
          colorGradient.setGradient("#404060", "#9090e0");
          colorGradient.setMidpoint(entries.length);

          this.jobs = {
            title: "Jobs",
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
