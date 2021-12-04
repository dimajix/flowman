<template>
  <v-container fluid>
    <v-row>
      <v-col  cols="3">
        <v-row>
          <job-project-chart
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
      <v-col  cols="3">
        <v-row>
          <job-name-chart
            :filter="filter"
          />
        </v-row>
        <v-row>
          <job-selector
            v-model="filter.jobs"
            :projects="filter.projects"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
      <v-col  cols="3">
        <v-row>
          <job-phase-chart
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
          <job-status-chart
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
import ProjectSelector from "@/components/ProjectSelector";
import JobSelector from "@/components/JobSelector";
import PhaseSelector from "@/components/PhaseSelector";
import StatusSelector from "@/components/StatusSelector";
import JobNameChart from "@/charts/JobNameChart.vue";
import JobPhaseChart from "@/charts/JobPhaseChart.vue";
import JobProjectChart from "@/charts/JobProjectChart.vue";
import JobStatusChart from "@/charts/JobStatusChart.vue";

export default {
  name: 'JobCharts',
  components: {
    ProjectSelector,
    JobSelector,
    PhaseSelector,
    StatusSelector,
    JobStatusChart,
    JobNameChart,
    JobPhaseChart,
    JobProjectChart
  },

  data() {
    return {
      filter: {
        jobs: [],
        projects: [],
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
