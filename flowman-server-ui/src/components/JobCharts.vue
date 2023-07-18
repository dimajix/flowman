<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col  cols="4">
        <v-row>
          <job-name-chart
            :filter="filter"
          />
        </v-row>
        <v-row>
          <job-selector
            v-model="filter.jobs"
            :project="project"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
      <v-col  cols="4">
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
      <v-col cols="4">
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
import JobSelector from "@/components/JobSelector";
import PhaseSelector from "@/components/PhaseSelector";
import StatusSelector from "@/components/StatusSelector";
import JobNameChart from "@/charts/JobNameChart.vue";
import JobPhaseChart from "@/charts/JobPhaseChart.vue";
import JobStatusChart from "@/charts/JobStatusChart.vue";

export default {
  name: 'JobCharts',
  components: {
    JobSelector,
    PhaseSelector,
    StatusSelector,
    JobStatusChart,
    JobNameChart,
    JobPhaseChart
  },

  props: {
    project: {
      type: String,
      default: () => ""
    },
  },

  watch: {
    project: function () {
      this.filter.projects = [this.project]
    },
  },

  data() {
    return {
      filter: {
        jobs: [],
        projects: [this.project],
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
