<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col cols="3">
        <job-status-chart/>
      </v-col>
      <v-col  cols="3">
        <job-phase-chart/>
      </v-col>
      <v-col  cols="3">
        <job-project-chart/>
      </v-col>
      <v-col  cols="3">
        <job-name-chart/>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
import JobNameChart from "@/charts/JobNameChart.vue";
import JobPhaseChart from "@/charts/JobPhaseChart.vue";
import JobProjectChart from "@/charts/JobProjectChart.vue";
import JobStatusChart from "@/charts/JobStatusChart.vue";

export default {
  name: 'JobCharts',
  components: {
    JobStatusChart,
    JobNameChart,
    JobPhaseChart,
    JobProjectChart
  },

  data() {
    return {
    };
  },

  mounted() {
  },

  methods: {
  }
};
</script>

<style>

</style>
