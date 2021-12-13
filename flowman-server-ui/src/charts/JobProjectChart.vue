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
          const colorGradient = new Gradient();
          colorGradient.setGradient("#407060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.projects = {
            title: "Projects",
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
