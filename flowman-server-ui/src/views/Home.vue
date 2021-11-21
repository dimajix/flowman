<template>
  <v-container
    fluid
  >
    <v-card>
      <v-card-title>Jobs Overview</v-card-title>
      <v-row>
        <v-col cols="6">
          <job-status-chart/>
        </v-col>
        <v-col cols="6">
          <job-phase-chart/>
        </v-col>
      </v-row>
    </v-card>
    <v-card>
      <v-card-title>Targets Overview</v-card-title>
      <v-row>
        <v-col cols="6">
          <target-status-chart/>
        </v-col>
        <v-col cols="6">
          <target-phase-chart/>
        </v-col>
      </v-row>
    </v-card>
  </v-container>
</template>

<script>
  import TargetStatusChart from "@/charts/TargetStatusChart";
  import TargetPhaseChart from "@/charts/TargetPhaseChart";
  import JobStatusChart from "@/charts/JobStatusChart";
  import JobPhaseChart from "@/charts/JobPhaseChart";

  export default {
    components: {
      JobPhaseChart,
      JobStatusChart,
      TargetStatusChart,
      TargetPhaseChart,
    },
  }
</script>
