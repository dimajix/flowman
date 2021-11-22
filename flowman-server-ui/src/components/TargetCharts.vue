<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col cols="3">
        <v-row>
          <target-project-chart
            :project-filter="projectFilter"
            :job-filter="jobFilter"
            :target-filter="targetFilter"
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
      <v-col cols="3">
        <v-row>
          <target-name-chart
            :project-filter="projectFilter"
            :job-filter="jobFilter"
            :target-filter="targetFilter"
            :status-filter="statusFilter"
            :phase-filter="phaseFilter"
          />
        </v-row>
        <v-row>
          <target-selector
            v-model="targetFilter"
          />
        </v-row>
      </v-col>
      <v-col cols="3">
        <v-row>
        <target-phase-chart
          :project-filter="projectFilter"
          :job-filter="jobFilter"
          :target-filter="targetFilter"
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
          <target-status-chart
            :project-filter="projectFilter"
            :job-filter="jobFilter"
            :target-filter="targetFilter"
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
      projectFilter: [],
      jobFilter: [],
      targetFilter: [],
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
