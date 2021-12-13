<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col cols="3">
        <v-row>
          <target-project-chart
            :filter="filter"
          />
        </v-row>
        <v-row>
          <project-selector
            v-model="filter.projects"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
      <v-col cols="3">
        <v-row>
          <target-name-chart
            :filter="filter"
          />
        </v-row>
        <v-row>
          <target-selector
            v-model="filter.targets"
            :projects="filter.projects"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
      <v-col cols="3">
        <v-row>
        <target-phase-chart
          :filter="filter"
        />
        </v-row>
        <v-row>
          <phase-selector
            v-model="filter.phases"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
      <v-col cols="3">
        <v-row>
          <target-status-chart
            :filter="filter"
          />
        </v-row>
        <v-row>
          <status-selector
            v-model="filter.status"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
import TargetNameChart from "@/charts/TargetNameChart.vue";
import TargetPhaseChart from "@/charts/TargetPhaseChart.vue";
import TargetProjectChart from "@/charts/TargetProjectChart.vue";
import TargetStatusChart from "@/charts/TargetStatusChart.vue";
import ProjectSelector from "@/components/ProjectSelector";
import TargetSelector from "@/components/TargetSelector";
import PhaseSelector from "@/components/PhaseSelector";
import StatusSelector from "@/components/StatusSelector";

export default {
  name: 'TargetCharts',
  components: {
    ProjectSelector,
    TargetSelector,
    PhaseSelector,
    StatusSelector,
    TargetStatusChart,
    TargetNameChart,
    TargetPhaseChart,
    TargetProjectChart
  },

  data() {
    return {
      filter: {
        jobs: [],
        projects: [],
        targets: [],
        phases: [],
        status: [],
      }
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
