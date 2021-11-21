<template>
  <v-container fluid>
    <v-subheader class="title" inset>Job Phase</v-subheader>
    <pie-chart
      height="160"
      v-if="phaseLoaded"
      :data="phaseData"
      :options="chartOptions">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'JobPhaseChart',
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
      phaseData: {},
      phaseLoaded: false,
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.phaseLoaded = false
      this.$api.getJobCounts('phase')
        .then(response => {
          this.phaseData = {
            labels: ["Validate", "Create", "Build", "Verify", "Truncate", "Destroy"],
            datasets: [
              {
                backgroundColor: ["#ffde09", "#daff00", "#00ff4e", "#00ffcd", "#e009ff", "#ff2800"],
                data: [
                  response.data["VALIDATE"],
                  response.data["CREATE"],
                  response.data["BUILD"],
                  response.data["VERIFY"],
                  response.data["TRUNCATE"],
                  response.data["DESTROY"]
                ]
              }
            ]
          }
          this.phaseLoaded = true
        })
    }
  }
};
</script>

<style>
</style>
