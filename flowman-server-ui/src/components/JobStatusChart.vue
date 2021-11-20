<template>
  <v-container>
    <v-row>
      <v-col>
        <pie-chart
          height="200px"
          v-if="statusLoaded"
          :data="statusData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
      <v-col>
        <pie-chart
          height="200px"
          v-if="phaseLoaded"
          :data="phaseData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
      <v-col>
        <pie-chart
          height="200px"
          v-if="phaseLoaded"
          :data="phaseData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'JobStatusChart',
  components: {
    PieChart
  },

  data() {
    return {
      chartOptions: {
        hoverBorderWidth: 20
      },
      statusLoaded: false,
      statusData: null,
      phaseData: null,
      phaseLoaded: false,
      jobData: null,
      jobLoaded: false
    };
  },

  mounted() {
    this.getData()
  },

  methods: {
    getData() {
      this.statusLoaded = false
      this.$api.getJobCounts('status')
        .then(response => {
          this.statusData = {
            hoverBackgroundColor: "red",
            hoverBorderWidth: 10,
            labels: ["Success", "Skipped", "Failed"],
            datasets: [
              {
                label: "Job Status",
                backgroundColor: ["#41B883", "#00D8FF", "#E46651"],
                data: [response.data["SUCCESS"],  response.data["SKIPPED"], response.data["FAILED"]]
              }
            ]
          }
          this.statusLoaded = true
        })

      this.phaseLoaded = false
      this.$api.getJobCounts('phase')
        .then(response => {
          this.phaseData = {
            hoverBackgroundColor: "red",
            hoverBorderWidth: 10,
            labels: ["Verify", "Create", "Build", "Validate", "Truncate", "Destroy"],
            datasets: [
              {
                label: "Job Phase",
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
