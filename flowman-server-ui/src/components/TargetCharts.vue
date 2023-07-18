<template>
  <v-container fluid>
    <v-row dense no-gutters>
      <v-col cols="4">
        <v-row>
          <target-name-chart
            :filter="filter"
          />
        </v-row>
        <v-row>
          <target-selector
            v-model="filter.targets"
            :project="project"
            @input='$emit("input", filter)'
          />
        </v-row>
      </v-col>
      <v-col cols="4">
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
      <v-col cols="4">
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
import TargetStatusChart from "@/charts/TargetStatusChart.vue";
import TargetSelector from "@/components/TargetSelector";
import PhaseSelector from "@/components/PhaseSelector";
import StatusSelector from "@/components/StatusSelector";

export default {
  name: 'TargetCharts',
  components: {
    TargetSelector,
    PhaseSelector,
    StatusSelector,
    TargetStatusChart,
    TargetNameChart,
    TargetPhaseChart,
  },

  props: {
    project: {
      type: String,
      default: () => ""
    }
  },

  watch: {
    project: function () { this.filter.projects = [this.project] },
  },

  data() {
    return {
      filter: {
        jobs: [],
        projects: [this.project],
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
