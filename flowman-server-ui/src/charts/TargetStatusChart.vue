<template>
  <v-container fluid>
    <pie-chart
      :height="height"
      v-if="loaded"
      :chart-data="status"
      :title-position="titlePosition"
    >
    </pie-chart>
  </v-container>
</template>

<script>
import PieChart from "@/charts/PieChart.js";
import Filter from "@/mixins/Filter.js";

export default {
  name: 'TargetStatusChart',
  mixins: [Filter],
  components: { PieChart },

  props: {
    titlePosition: {
      type: String,
      default: () => 'top'
    },
    height: {
      type: Number,
      default: () => 180
    }
  },

  data() {
    return {
      status: {},
      loaded: false,
    };
  },

  methods: {
    getData() {
      this.$api.getTargetCounts('status', this.projectFilter, this.jobFilter, this.targetFilter, this.phaseFilter, this.statusFilter)
        .then(response => {
          this.status = {
            title: "Status",
            labels: ["Running", "Success", "Skipped", "Failed"],
            datasets: [
              {
                backgroundColor: ["#90D070", "#70B050", "#80A0D0", "#E42651"],
                data: [response.data["RUNNING"], response.data["SUCCESS"],  response.data["SKIPPED"], response.data["FAILED"]]
              }
            ]
          }
          this.loaded = true
        })
    }
  }
};
</script>

<style>
</style>
