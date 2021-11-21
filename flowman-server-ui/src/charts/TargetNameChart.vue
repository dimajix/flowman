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
