<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col cols="3">
        <v-subheader class="title" inset>Target Status</v-subheader>
        <pie-chart
          height=160
          v-if="statusLoaded"
          :data="statusData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
      <v-col cols="3">
        <v-subheader class="title" inset>Target Phase</v-subheader>
        <pie-chart
          height=160
          v-if="phaseLoaded"
          :data="phaseData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
      <v-col cols="3">
        <v-subheader class="title" inset>Project Name</v-subheader>
        <pie-chart
          height=160
          v-if="projectLoaded"
          :data="projectData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
      <v-col cols="3">
        <v-subheader class="title" inset>Target Name</v-subheader>
        <pie-chart
          height=160
          v-if="targetLoaded"
          :data="targetData"
          :options="chartOptions">
        </pie-chart>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'TargetStatusChart',
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
      statusData: {},
      statusLoaded: false,
      phaseData: {},
      phaseLoaded: false,
      projectData: {},
      projectLoaded: false,
      targetData: {},
      targetLoaded: false
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
                backgroundColor: ["#41B883", "#00D8FF", "#E46651"],
                data: [response.data["SUCCESS"],  response.data["SKIPPED"], response.data["FAILED"]]
              }
            ]
          }
          this.statusLoaded = true
        })

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

      this.projectLoaded = false
      this.$api.getJobCounts('project')
        .then(response => {
          this.projectData = {
            labels: Object.keys(response.data),
            datasets: [
              {
                backgroundColor: ["#11B883", "#00D8FF", "#E46651", "#E42651", "#E46611", "#846651"],
                data: Object.values(response.data)
              }
            ]
          }
          this.projectLoaded = true
        })

      this.targetLoaded = false
      this.$api.getTargetCounts('target')
        .then(response => {
          this.targetData = {
            labels: Object.keys(response.data),
            datasets: [
              {
                backgroundColor: ["#11B883", "#00D8FF", "#E46651", "#E42651", "#E46611", "#846651"],
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
