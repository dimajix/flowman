<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Name</v-subheader>
    <pie-chart
      height=160
      v-if="targetLoaded"
      :data="targetData">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";

export default {
  name: 'TargetNameChart',
  components: { PieChart },

  props: {
    projectFilter:  { type: Array },
    jobFilter: { type: Array },
    targetFilter: { type: Array },
    statusFilter: { type: Array },
    phaseFilter: { type: Array },
  },

  data() {
    return {
      targetData: {},
      targetLoaded: false
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
      this.targetLoaded = false
      this.$api.getTargetCounts('target', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          const colorGradient = new Gradient();
          colorGradient.setGradient("#404060", "#9090e0");
          colorGradient.setMidpoint(Object.values(response.data).length);

          this.targetData = {
            labels: Object.keys(response.data),
            datasets: [
              {
                backgroundColor: colorGradient.getArray(),
                data: Object.values(response.data)
              }
            ]
          }
          this.targetLoaded = true
        })
    }
  }
};
</script>

<style>
</style>
