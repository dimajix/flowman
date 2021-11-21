<template>
  <v-container fluid>
    <v-subheader class="title" inset>Project Name</v-subheader>
    <pie-chart
      height="160"
      v-if="projectLoaded"
      :data="projectData"
      :options="chartOptions">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'JobProjectChart',
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
    }
  }
};
</script>

<style>

</style>
