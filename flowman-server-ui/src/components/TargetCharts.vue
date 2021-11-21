<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col cols="3">
        <target-status-chart/>
      </v-col>
      <v-col cols="3">
        <target-phase-chart/>
      </v-col>
      <v-col cols="3">
        <target-project-chart/>
      </v-col>
      <v-col cols="3">
        <target-name-chart/>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
import TargetNameChart from "@/charts/TargetNameChart.vue";
import TargetPhaseChart from "@/charts/TargetPhaseChart.vue";
import TargetProjectChart from "@/charts/TargetProjectChart.vue";
import TargetStatusChart from "@/charts/TargetStatusChart.vue";

export default {
  name: 'TargetCharts',
  components: {
    TargetStatusChart,
    TargetNameChart,
    TargetPhaseChart,
    TargetProjectChart
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
