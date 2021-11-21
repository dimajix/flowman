<template>
  <v-container fluid>
    <v-subheader class="title" inset>Project Name</v-subheader>
    <pie-chart
      height=160
      v-if="projectLoaded"
      :data="projectData"
      :options="chartOptions">
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
      projectData: {},
      projectLoaded: false,
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.projectLoaded = false
      this.$api.getJobCounts('project')
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
