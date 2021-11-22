<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Status</v-subheader>
    <pie-chart
      height=160
      v-if="statusLoaded"
      :data="statusData">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'TargetStatusChart',
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
      statusData: {},
      statusLoaded: false,
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
      this.statusLoaded = false
      this.$api.getTargetCounts('status', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
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
