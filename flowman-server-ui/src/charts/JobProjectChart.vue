<template>
  <v-container fluid>
    <pie-chart
      :height="height"
      v-if="loaded"
      :chart-data="projects"
      :title-position="titlePosition"
    >
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";
import Filter from "@/mixins/Filter.js";

export default {
  name: 'JobProjectChart',
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
      projects: {},
    };
  },

  methods: {
    getData() {
      this.$api.getJobCounts('project', this.filter.projects, this.filter.jobs, this.filter.phases, this.filter.status)
        .then(response => {
          let entries = Object.entries(response.data).sort((l,r) => l[1] <= r[1])
          if (entries.length > 9) {
            let remaining = entries.slice(8)
            let remainingTotal = remaining.map(e => e[1]).reduce((a,v) => a+v, 0)
            let remainingCount = remaining.length
            entries = entries.slice(0, 8).concat([[remainingCount + " more projects...", remainingTotal]])
          }

          const colorGradient = new Gradient();
          colorGradient.setGradient("#407060", "#9090e0");
          colorGradient.setMidpoint(entries.length);

          this.projects = {
            title: "Projects",
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
