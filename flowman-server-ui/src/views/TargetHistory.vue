<template>
  <v-container
    fluid
  >
    <v-card>
      <target-charts
        v-model="filter"
      />
    </v-card>
    <v-card>
      <v-card-title>Target History</v-card-title>
      <v-data-table
        dense
        :headers="headers"
        :items="targets"
        :items-per-page="25"
        :loading="loading"
        item-key="id"
        class="elevation-1"
      >
        <template v-slot:item.status="{ item }">
          <v-icon>
            {{ getIcon(item.status) }}
          </v-icon>
          {{ item.status }}
        </template>
      </v-data-table>
    </v-card>
  </v-container>
</template>

<script>
  import TargetCharts from "@/components/TargetCharts";

  export default {
    components: {TargetCharts},
    props: {
    },

    data() {
      return {
        targets: [],
        total: 0,
        loading: false,
        filter: {
          projects: [],
          jobs: [],
          targets: [],
          phases: [],
          status: [],
        },
        headers: [
          {text: 'Target Run ID', value: 'id'},
          {text: 'Job Run ID', value: 'jobId'},
          {text: 'Project Name', value: 'project'},
          {text: 'Project Version', value: 'version'},
          {text: 'Target Name', value: 'target'},
          {text: 'Build Phase', value: 'phase'},
          {text: 'Status', value: 'status'},
          {text: 'Started at', value: 'startDateTime'},
          {text: 'Finished at', value: 'endDateTime'},
          {text: 'Error message', value: 'error'},
        ]
      }
    },

    computed: {
      projectFilter() { return this.filter.projects },
      jobFilter() { return this.filter.jobs },
      targetFilter() { return this.filter.targets },
      phaseFilter() { return this.filter.phases },
      statusFilter() { return this.filter.status }
    },

    watch: {
      projectFilter: function () { this.getData() },
      jobFilter: function () { this.getData() },
      targetFilter: function () { this.getData() },
      phaseFilter: function () { this.getData() },
      statusFilter: function () { this.getData() },
    },

    mounted() {
      this.getData()
    },

    methods: {
      getData() {
        this.loading = true
        this.$api.getTargetsHistory(this.filter.projects, this.filter.jobs, this.filter.targets, this.filter.phases, this.filter.status)
          .then(response => {
            this.targets = response.data
            this.total = response.total
            this.loading = false
          })
      },

      getIcon(status) {
        if (status === "SUCCESS") {
          return "done_all"
        }
        else if (status === "SKIPPED") {
          return "fast_forward"
        }
        else if (status === "FAILED") {
          return "error"
        }
        else {
          return "warning_amber"
        }
      }
    }
  }
</script>

<style>

</style>
