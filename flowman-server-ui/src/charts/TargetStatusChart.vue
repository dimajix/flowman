<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Status</v-subheader>
    <pie-chart
      height=160
      v-if="statusLoaded"
      :data="statusData"
      :options="chartOptions">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'TargetStatusChart',
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
      statusData: {},
      statusLoaded: false,
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.statusLoaded = false
      this.$api.getTargetCounts('status')
        .then(response => {
          this.statusData = {
            labels: ["Success", "Skipped", "Failed"],
            datasets: [
              {
                backgroundColor: ["#41B883", "#00D8FF", "#E42651"],
                data: [response.data["SUCCESS"],  response.data["SKIPPED"], response.data["FAILED"]]
              }
            ]
          }
          this.statusLoaded = true
        })
    }
  }
};
</script>

<style>

</style>
