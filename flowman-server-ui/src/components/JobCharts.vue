<template>
  <v-container fluid>
    <v-row>
      <v-col  cols="3">
        <v-row>
          <job-project-chart
            :job-filter="jobFilter"
            :project-filter="projectFilter"
            :status-filter="statusFilter"
            :phase-filter="phaseFilter"
          />
        </v-row>
        <v-row>
          <project-selector
            v-model="projectFilter"
          />
        </v-row>
      </v-col>
      <v-col  cols="3">
        <v-row>
          <job-name-chart
            :job-filter="jobFilter"
            :project-filter="projectFilter"
            :status-filter="statusFilter"
            :phase-filter="phaseFilter"
          />
        </v-row>
        <v-row>
          <job-selector
            v-model="jobFilter"
          />
        </v-row>
      </v-col>
      <v-col  cols="3">
        <v-row>
          <job-phase-chart
            :job-filter="jobFilter"
            :project-filter="projectFilter"
            :status-filter="statusFilter"
            :phase-filter="phaseFilter"
          />
        </v-row>
        <v-row>
          <phase-selector
            v-model="phaseFilter"
          />
        </v-row>
      </v-col>
      <v-col cols="3">
        <v-row>
          <job-status-chart
            :job-filter="jobFilter"
            :project-filter="projectFilter"
            :status-filter="statusFilter"
            :phase-filter="phaseFilter"
          />
        </v-row>
        <v-row>
          <status-selector
            v-model="statusFilter"
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
      jobFilter: [],
      projectFilter: [],
      phaseFilter: [],
      statusFilter: [],
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
