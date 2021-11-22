<template>
  <v-container fluid>
    <v-subheader class="title" inset>Target Phase</v-subheader>
    <pie-chart
      height=160
      v-if="phaseLoaded"
      :data="phaseData">
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";

export default {
  name: 'TargetPhaseChart',
  components: { PieChart  },

  props: {
    projectFilter:  { type: Array },
    jobFilter: { type: Array },
    targetFilter: { type: Array },
    statusFilter: { type: Array },
    phaseFilter: { type: Array },
  },

  data() {
    return {
      phaseData: {},
      phaseLoaded: false,
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
      this.phaseLoaded = false
      this.$api.getTargetCounts('phase', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
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
