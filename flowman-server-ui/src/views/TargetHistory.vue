<template>
  <v-container
    fluid
  >
    <v-row>
      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <target-charts
            v-model="filter"
          />
        </v-card>
      </v-col>

      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <v-card-title>Target History</v-card-title>
          <v-data-table
            :headers="headers"
            :items="targets"
            :loading="loading"
            :options.sync="options"
            :server-items-length="total"
            item-key="id"
            class="elevation-1"
            :footer-props="{
              prevIcon: 'navigate_before',
              nextIcon: 'navigate_next'
            }"
          >
            <template v-slot:item.status="{ item }">
              <v-icon>
                {{ getIcon(item.status) }}
              </v-icon>
              {{ item.status }}
            </template>
            <template v-slot:item.partitions="{ item }">
              <v-chip
                v-for="p in Object.entries(item.partitions) "
                :key="p"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </template>
          </v-data-table>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
  import TargetCharts from "@/components/TargetCharts";
  import Filter from "@/mixins/Filter.js";

  export default {
    name: "TargetHistory",
    mixins: [Filter],
    components: {TargetCharts},

    data() {
      return {
        targets: [],
        total: 0,
        options: {},
        loading: false,
        headers: [
          {text: 'Target Run ID', value: 'id'},
          {text: 'Job Run ID', value: 'jobId'},
          {text: 'Project Name', value: 'project'},
          {text: 'Project Version', value: 'version'},
          {text: 'Target Name', value: 'target'},
          {text: 'Partition', value: 'partitions'},
          {text: 'Build Phase', value: 'phase'},
          {text: 'Status', value: 'status', width:120},
          {text: 'Started at', value: 'startDateTime'},
          {text: 'Finished at', value: 'endDateTime'},
          {text: 'Error message', value: 'error', width:160},
        ]
      }
    },

    watch: {
      options: {
        handler () {
          this.getData()
        },
        deep: true,
      },
    },

    methods: {
      getData() {
        const { page, itemsPerPage } = this.options
        const offset = (page-1)*itemsPerPage

        this.loading = true
        this.$api.getTargetsHistory(this.filter.projects, this.filter.jobs, this.filter.targets, this.filter.phases, this.filter.status, offset, itemsPerPage)
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
