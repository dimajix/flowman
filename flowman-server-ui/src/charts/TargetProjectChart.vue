<template>
  <v-container fluid>
    <v-subheader class="title" inset>Project Name</v-subheader>
    <pie-chart
      height=160
      v-if="projectLoaded"
      :data="projectData">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";

export default {
  name: 'TargetProjectChart',
  components: {
    PieChart
  },

  props: {
    projectFilter:  { type: Array },
    jobFilter: { type: Array },
    targetFilter: { type: Array },
    statusFilter: { type: Array },
    phaseFilter: { type: Array },
  },

  data() {
    return {
      projectData: {},
      projectLoaded: false,
    };
  },

  watch: {
    projectFilter: function () { this.getData() },
    jobFilter: function () { this.getData() },
    targetFilter: function () { this.getData() },
    statusFilter: function () { this.getData() },
    phaseFilter: function () { this.getData() },
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.projectLoaded = false
      this.$api.getTargetCounts('project', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          const colorGradient = new Gradient();
          colorGradient.setGradient("#407060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.projectData = {
            labels: Object.keys(response.data),
            datasets: [
              {
                backgroundColor: colorGradient.getArray(),
                data: Object.values(response.data)
              }
            ]
          }
          this.projectLoaded = true
        })
    }
  }
};
</script>

<style>

</style>
