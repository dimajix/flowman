<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Phase</v-subheader>
    <pie-chart
      height=160
      v-if="phaseLoaded"
      :data="phaseData"
      :options="chartOptions">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'TargetPhaseChart',
  components: { PieChart  },

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
      this.$api.getTargetCounts('phase')
        .then(response => {
          this.phaseData = {
            labels: ["Verify", "Create", "Build", "Validate", "Truncate", "Destroy"],
            datasets: [
              {
                backgroundColor: ["#11B883", "#00D8FF", "#E46651", "#E42651", "#E46611", "#846651"],
                data: [
                  response.data["VERIFY"],
                  response.data["CREATE"],
                  response.data["BUILD"],
                  response.data["VALIDATE"],
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
