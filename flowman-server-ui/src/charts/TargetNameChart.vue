<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Name</v-subheader>
    <pie-chart
      height=160
      v-if="targetLoaded"
      :data="targetData"
      :options="chartOptions">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Gradient from "javascript-color-gradient";

export default {
  name: 'TargetNameChart',
  components: { PieChart },

  data() {
    return {
      chartOptions: {
        responsive: true,
        maintainAspectRatio: true,
        hoverBorderWidth: 20,
        legend: {
          position: 'right',
          align: 'center'
        },
      },
      targetData: {},
      targetLoaded: false
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.targetLoaded = false
      this.$api.getTargetCounts('target')
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
